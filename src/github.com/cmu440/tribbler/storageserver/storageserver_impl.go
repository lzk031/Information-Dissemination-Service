package storageserver

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/storagerpc"
)

const (
	Delete = iota + 1
	Put
	AppendToList
	RemoveFromList
)

type storageServer struct {
	lsRPC          map[string]*rpc.Client // libstore hostport to rpc client
	ItemMap        map[string]string      // PostKey to value
	ListMap        map[string][]string    // key defined in util
	numNodes       int                    // number of storage servers expected
	ServerNodes    []storagerpc.Node      // only for master server
	AllServerReady bool                   // only for master server
	Mutex          map[string]*sync.Mutex // mutex for each key
	ServerMutex    *sync.Mutex
	nodeID         uint32
	// critical section
	CacheRecord map[string][]LeaseRecord // key to libstore server hostport

	Ready chan bool // only for slave servers
	// addRecord    chan AddPack    // key and item
	// ModifyCS     chan ModifyPack // key
	// ModifyReply  chan storagerpc.Status
	// successReply chan bool
	// addRPC       chan string // HostPort
}

type LeaseRecord struct {
	HostPort  string
	timestamp time.Time
}

type AddPack struct {
	Key         string
	LeaseRecord LeaseRecord
}

type ModifyPack struct {
	Operation int
	Key       string
	Value     string
}

type NodeSlice []storagerpc.Node

func (key NodeSlice) Len() int {
	return len(key)
}

func (key NodeSlice) Swap(i, j int) {
	key[i], key[j] = key[j], key[i]
	return
}

func (key NodeSlice) Less(i, j int) bool {
	return key[i].NodeID < key[j].NodeID // in asc order
}

// NewStorageServer creates and starts a new StorageServer. masterServerHostPort
// is the master storage server's host:port address. If empty, then this server
// is the master; otherwise, this server is a slave. numNodes is the total number of
// servers in the ring. port is the port number that this server should listen on.
// nodeID is a random, unsigned 32-bit ID identifying this server.
//
// This function should return only once all storage servers have joined the ring,
// and should return a non-nil error if the storage server could not be started.
func NewStorageServer(masterServerHostPort string, numNodes, port int, nodeID uint32) (StorageServer, error) {
	// defer fmt.Println("StorageServer return")
	storageServer := new(storageServer)
	storageServer.numNodes = numNodes
	storageServer.ItemMap = make(map[string]string)
	storageServer.ListMap = make(map[string][]string)
	storageServer.lsRPC = make(map[string]*rpc.Client)
	storageServer.CacheRecord = make(map[string][]LeaseRecord)
	storageServer.Mutex = make(map[string]*sync.Mutex)
	storageServer.ServerMutex = &sync.Mutex{}
	storageServer.nodeID = nodeID

	// storageServer.addRecord = make(chan AddPack) // key and item
	// storageServer.successReply = make(chan bool)
	// storageServer.addRPC = make(chan string)
	// storageServer.ModifyReply = make(chan storagerpc.Status)
	// storageServer.ModifyCS = make(chan ModifyPack)

	hostPort := net.JoinHostPort("localhost", strconv.Itoa(port))
	listener, err := net.Listen("tcp", hostPort)
	if err != nil {
		fmt.Println("Error on StorageServer Listen", err)
		return nil, err
	}

	if masterServerHostPort == "" { // master server
		storageServer.ServerNodes = make([]storagerpc.Node, 0)
		// register itself
		node := &storagerpc.Node{NodeID: nodeID, HostPort: hostPort}
		storageServer.ServerNodes = append(storageServer.ServerNodes, *node)
		// Wrap the Server before registering it for RPC.
		if err = rpc.RegisterName("StorageServer", storagerpc.Wrap(storageServer)); err != nil {
			fmt.Println("Error on StorageServer RegisterName", err)
			return nil, err
		}
		rpc.HandleHTTP()
		go http.Serve(listener, nil)
		// go storageServer.LeaseHandler()
		return storageServer, nil
	}

	// slave server
	storageServer.Ready = make(chan bool)
	slave, err := rpc.DialHTTP("tcp", masterServerHostPort)
	if err != nil {
		fmt.Println("Error: Slave StorageServer DialHTTP:", err)
		return nil, err
	}
	go func() {
		node := &storagerpc.Node{HostPort: hostPort, NodeID: nodeID}
		args := &storagerpc.RegisterArgs{ServerInfo: *node}
		var reply storagerpc.RegisterReply
		for {
			_ = slave.Call("StorageServer.RegisterServer", args, &reply)
			if reply.Status != storagerpc.OK {
				time.Sleep(1 * time.Second)
			} else { // Status = OK
				storageServer.ServerNodes = reply.Servers
				storageServer.Ready <- true
				return
			}
		}
	}()
	// block slave servers
	_ = <-storageServer.Ready
	if err = rpc.RegisterName("StorageServer", storagerpc.Wrap(storageServer)); err != nil {
		fmt.Println("Error on StorageServer RegisterName", err)
		return nil, err
	}
	// Setup the HTTP handler that will server incoming RPCs and
	// serve requests in a background goroutine.
	rpc.HandleHTTP()
	go http.Serve(listener, nil)
	// go storageServer.LeaseHandler()
	return storageServer, nil
}

// func (ss *storageServer) LeaseHandler() {
// 	for {
// 		select {
// 		case pack := <-ss.addRecord: // Get, GetList
// 			status := ss.AddRecord(pack)
// 			ss.successReply <- status
// 		case HostPort := <-ss.addRPC: // Get, GetList
// 			Sussess := ss.AddRPC(HostPort)
// 			// if !Sussess {
// 			// 	fmt.Println("Error on AddRPC")
// 			// 	return
// 			// }
// 			ss.successReply <- Sussess
// 		case pack := <-ss.ModifyCS: // Delete, Put, AppendToList, RemoveFromList
// 			status := ss.Modify(pack)
// 			ss.ModifyReply <- status
// 		}
// 	}
// }

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	if ss.AllServerReady {
		reply.Status = storagerpc.OK
		reply.Servers = ss.ServerNodes
		return nil
	}
	found := false
	for _, ServerNode := range ss.ServerNodes {
		if ServerNode == args.ServerInfo {
			found = true
			break
		}
	}
	if !found { // new register request
		node := &storagerpc.Node{NodeID: args.ServerInfo.NodeID, HostPort: args.ServerInfo.HostPort}
		ss.ServerNodes = append(ss.ServerNodes, *node)
	}
	if len(ss.ServerNodes) == ss.numNodes { // all servers available
		// sort ServerNodes
		ServerNodes := ss.ServerNodes
		NodeSlice := NodeSlice(ServerNodes)
		sort.Sort(NodeSlice)
		ss.ServerNodes = []storagerpc.Node(NodeSlice)
		reply.Servers = ss.ServerNodes
		reply.Status = storagerpc.OK
		return nil
	}

	reply.Status = storagerpc.NotReady
	return nil
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	// defer fmt.Println("GetServers Done")
	if len(ss.ServerNodes) == ss.numNodes { // all servers available
		reply.Status = storagerpc.OK
		reply.Servers = ss.ServerNodes
		return nil
	}
	reply.Status = storagerpc.NotReady
	reply.Servers = ss.ServerNodes
	return nil
}

func (ss *storageServer) AddRecord(pack AddPack) bool {
	ss.Lock(pack.Key)
	defer ss.Unlock(pack.Key)
	RecordPackSlice, found := ss.CacheRecord[pack.Key]
	if !found {
		var leaseRecord []LeaseRecord
		leaseRecord = append(leaseRecord, pack.LeaseRecord)
		ss.ServerMutex.Lock()
		ss.CacheRecord[pack.Key] = leaseRecord
		ss.ServerMutex.Unlock()
		return true
	}
	// if already in record, update timestamp
	for i, recordPack := range RecordPackSlice {
		if recordPack.HostPort == pack.LeaseRecord.HostPort {
			recordPack.timestamp = pack.LeaseRecord.timestamp
			RecordPackSlice[i] = recordPack
			ss.CacheRecord[pack.Key] = RecordPackSlice
			return true
		}
	}
	// exists some libserver that has leases but this libserver does not
	RecordPackSlice = append(RecordPackSlice, pack.LeaseRecord)
	ss.ServerMutex.Lock()
	ss.CacheRecord[pack.Key] = RecordPackSlice
	ss.ServerMutex.Unlock()
	return true
}

func (ss *storageServer) AddRPC(HostPort string) bool {
	if _, found := ss.lsRPC[HostPort]; !found {
		lsRPC, derr := rpc.DialHTTP("tcp", HostPort)
		if derr != nil {
			return false
		}
		ss.ServerMutex.Lock()
		ss.lsRPC[HostPort] = lsRPC // add rpc client
		ss.ServerMutex.Unlock()
	}
	return true
}

func (ss *storageServer) LeaseMaker(args *storagerpc.GetArgs) storagerpc.Lease {
	if !args.WantLease {
		Lease := &storagerpc.Lease{Granted: false, ValidSeconds: storagerpc.LeaseSeconds}
		return *Lease
	}
	// WantLease
	// update hostport
	// ss.addRPC <- args.HostPort // wait for adding RPC
	_ = ss.AddRPC(args.HostPort)
	// <-ss.successReply
	// renew timestamp
	leaserecord := &LeaseRecord{timestamp: time.Now(), HostPort: args.HostPort}
	addPack := &AddPack{Key: args.Key, LeaseRecord: *leaserecord}
	// ss.addRecord <- *addPack // wait for adding lease record
	_ = ss.AddRecord(*addPack)
	// <-ss.successReply

	Lease := &storagerpc.Lease{Granted: true, ValidSeconds: storagerpc.LeaseSeconds}
	return *Lease
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	// fmt.Println("Get")
	// defer fmt.Println("Get return")
	if right := ss.DoubleCheckKey(args.Key); !right {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	if _, found := ss.ItemMap[args.Key]; found {
		lease := ss.LeaseMaker(args)
		reply.Status = storagerpc.OK
		// key := ss.ParseKey(args.Key)
		reply.Value = ss.ItemMap[args.Key]
		reply.Lease = lease
		return nil
	}
	reply.Status = storagerpc.KeyNotFound
	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	// assuem userID exists
	if right := ss.DoubleCheckKey(args.Key); !right {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	lease := ss.LeaseMaker(args)
	reply.Status = storagerpc.OK
	// key := ss.ParseKey(args.Key)
	reply.Value = ss.ListMap[args.Key]
	reply.Lease = lease
	return nil
}

func (ss *storageServer) Modify(pack ModifyPack) storagerpc.Status {
	switch pack.Operation {
	case Delete:
		_ = ss.CheckCallBack(pack.Key)
		if _, found := ss.ItemMap[pack.Key]; found {
			ss.ServerMutex.Lock()
			delete(ss.ItemMap, pack.Key)
			ss.ServerMutex.Unlock()
			return storagerpc.OK
		}
		// not found
		return storagerpc.ItemNotFound
	case Put:
		_ = ss.CheckCallBack(pack.Key)
		ss.ServerMutex.Lock()
		ss.ItemMap[pack.Key] = pack.Value
		ss.ServerMutex.Unlock()
		return storagerpc.OK
	case AppendToList:
		_ = ss.CheckCallBack(pack.Key)
		list := ss.ListMap[pack.Key]
		i := FindPos(list, pack.Value)
		if i != -1 { // already exists
			return storagerpc.ItemExists
		}
		list = append(list, pack.Value)
		ss.ServerMutex.Lock()
		ss.ListMap[pack.Key] = list
		ss.ServerMutex.Unlock()
		return storagerpc.OK
	case RemoveFromList:
		_ = ss.CheckCallBack(pack.Key)
		list := ss.ListMap[pack.Key]
		i := FindPos(list, pack.Value)
		if i == -1 { // not found in slice
			return storagerpc.ItemNotFound
		}
		list = append(list[:i], list[i+1:]...)
		ss.ServerMutex.Lock()
		ss.ListMap[pack.Key] = list
		ss.ServerMutex.Unlock()
		return storagerpc.OK
	}
	return storagerpc.OK
}

func (ss *storageServer) CheckCallBack(key string) bool {
	LeaseRecordSlice, found := ss.CacheRecord[key]
	// fmt.Println(ss.CacheRecord)
	// fmt.Println(found)
	if found {
		Return := make(chan bool)
		GoRoutine := 0
		ReturnRoutine := 0
		for _, LeaseRecord := range LeaseRecordSlice {
			GoRoutine++
			go ss.CallBackOneLease(LeaseRecord, key, Return)
		}
		for {
			// fmt.Println("CheckCallBack3")
			select {
			case <-Return:
				ReturnRoutine++
				// fmt.Println("CheckCallBack4")
				if ReturnRoutine == GoRoutine {
					ss.ServerMutex.Lock()
					// fmt.Println(ss.CacheRecord)
					delete(ss.CacheRecord, key)
					// fmt.Println(ss.CacheRecord)
					ss.ServerMutex.Unlock()
					return true
				}
			}
		}
	}
	return true
}

func (ss *storageServer) CallBackOneLease(LeaseRecord LeaseRecord, key string, Return chan bool) {
	duration := time.Since(LeaseRecord.timestamp).Seconds()
	if expire := storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds - duration; expire > 0 {
		CallBackFinish := make(chan bool)
		go ss.LeaseCallBack(key, LeaseRecord.HostPort, CallBackFinish)
		// fmt.Println("CallBackOneLease1")
		select {
		case <-CallBackFinish:
			// fmt.Println("callback finish")
		case <-time.After(time.Duration(storagerpc.LeaseSeconds+storagerpc.LeaseGuardSeconds) * time.Second):
			close(CallBackFinish)
			// fmt.Println("timeout")
		}
	}
	Return <- true
	return
}

func (ss *storageServer) LeaseCallBack(key string, HostPort string, Finish chan bool) storagerpc.Status {
	lsRPC := ss.lsRPC[HostPort]
	args := &storagerpc.RevokeLeaseArgs{Key: key}
	var reply storagerpc.RevokeLeaseReply
	// fmt.Println("before rpc")
	lsRPC.Call("LeaseCallbacks.RevokeLease", args, &reply)
	// fmt.Println("before finish")
	Finish <- true
	// fmt.Println("after finish")
	return reply.Status
}

func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	ss.Lock(args.Key)
	defer ss.Unlock(args.Key)
	if right := ss.DoubleCheckKey(args.Key); !right {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	// assume key in ItemMap
	// key := ss.ParseKey(args.Key)
	pack := &ModifyPack{Operation: Delete, Key: args.Key, Value: ""}
	status := ss.Modify(*pack)
	// ss.ModifyCS <- *pack
	// status := <-ss.ModifyReply
	reply.Status = status
	return nil
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	ss.Lock(args.Key)
	defer ss.Unlock(args.Key)
	if right := ss.DoubleCheckKey(args.Key); !right {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	// fmt.Println("Put")
	// defer fmt.Println("Put return")
	// check key lease
	// key := ss.ParseKey(args.Key)
	pack := &ModifyPack{Operation: Put, Key: args.Key, Value: args.Value}
	status := ss.Modify(*pack)
	// ss.ModifyCS <- *pack
	// status := <-ss.ModifyReply
	reply.Status = status
	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	ss.Lock(args.Key)
	defer ss.Unlock(args.Key)
	if right := ss.DoubleCheckKey(args.Key); !right {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	// assume userID exists
	// key := ss.ParseKey(args.Key)
	pack := &ModifyPack{Operation: AppendToList, Key: args.Key, Value: args.Value}
	status := ss.Modify(*pack)
	// ss.ModifyCS <- *pack
	// status := <-ss.ModifyReply
	reply.Status = status
	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	ss.Lock(args.Key)
	defer ss.Unlock(args.Key)
	if right := ss.DoubleCheckKey(args.Key); !right {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	// assume userID exists
	// key := ss.ParseKey(args.Key)
	pack := &ModifyPack{Operation: RemoveFromList, Key: args.Key, Value: args.Value}
	status := ss.Modify(*pack)
	// ss.ModifyCS <- *pack
	// status := <-ss.ModifyReply
	reply.Status = status
	return nil
}

// func (ss *storageServer) ParseKey(key string) string {
// 	return strings.Split(key, ":")[0]
// }
func (ss *storageServer) DoubleCheckKey(key string) bool {
	NodeNum := libstore.StoreHash(key)
	for _, ServerNode := range ss.ServerNodes {
		if ServerNode.NodeID >= NodeNum {
			return ServerNode.NodeID == ss.nodeID
		}
	}
	FirstNode := ss.ServerNodes[0]
	// if cannot find, choose the first node
	return FirstNode.NodeID == ss.nodeID
}

func (ss *storageServer) Lock(key string) {
	Mutex, found := ss.Mutex[key]
	if !found {
		mutex := &sync.Mutex{}
		ss.Mutex[key] = mutex
		mutex.Lock()
		return
	}
	Mutex.Lock()
	return
}

func (ss *storageServer) Unlock(key string) {
	mutex := ss.Mutex[key]
	mutex.Unlock()
	return
}

func FindPos(s []string, value string) int {
	for p, v := range s {
		if v == value {
			return p
		}
	}
	return -1
}
