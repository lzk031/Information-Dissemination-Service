package storageserver

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"strconv"
	"time"

	"github.com/cmu440/tribbler/rpc/storagerpc"
)

type storageServer struct {
	lsRPC          map[string]*rpc.Client // libstore hostport to rpc client
	ItemMap        map[string]string      // PostKey to value
	ListMap        map[string][]string    // key defined in util
	numNodes       int                    // number of storage servers expected
	ServerNodes    []storagerpc.Node      // only for master server
	AllServerReady bool                   // only for master server
	// critical section
	CacheRecord map[string][]LeaseRecord // key to libstore server hostport

	Ready        chan bool    // only for slave servers
	addRecord    chan AddPack // key and item
	delRecord    chan string  // key
	CallBack     chan string  // key
	successReply chan bool
	addRPC       chan string // HostPort
}

type LeaseRecord struct {
	HostPort  string
	timestamp time.Time
}

type AddPack struct {
	Key         string
	LeaseRecord LeaseRecord
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
	defer fmt.Println("StorageServer return")
	storageServer := new(storageServer)
	storageServer.numNodes = numNodes
	storageServer.ItemMap = make(map[string]string)
	storageServer.ListMap = make(map[string][]string)
	storageServer.lsRPC = make(map[string]*rpc.Client)
	storageServer.CacheRecord = make(map[string][]LeaseRecord)

	storageServer.addRecord = make(chan AddPack) // key and item
	storageServer.delRecord = make(chan string)  // key
	storageServer.CallBack = make(chan string)   // key
	storageServer.successReply = make(chan bool)
	storageServer.addRPC = make(chan string)

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
	go storageServer.LeaseHandler()
	return storageServer, nil
}

func (ss *storageServer) LeaseHandler() {
	for {
		select {
		case pack := <-ss.addRecord:
			status := ss.AddRecord(pack)
			ss.successReply <- status
		case HostPort := <-ss.addRPC:
			Sussess := ss.AddRPC(HostPort)
			if !Sussess {
				fmt.Println("Error on AddRPC")
				return
			}
			ss.successReply <- Sussess
		case key := <-ss.CallBack:
			status := ss.LeaseCallBack(key)
			ss.successReply <- status
			// case key := <-ss.delRecord:
			// 	delete(ss.CacheRecord, key)
			// 	ss.successReply <- true
		}
	}
}

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
	defer fmt.Println("GetServers Done")
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
	RecordPackSlice, found := ss.CacheRecord[pack.Key]
	if !found {
		var leaseRecord []LeaseRecord
		leaseRecord = append(leaseRecord, pack.LeaseRecord)
		ss.CacheRecord[pack.Key] = leaseRecord
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
	ss.CacheRecord[pack.Key] = RecordPackSlice
	return true
}

func (ss *storageServer) AddRPC(HostPort string) bool {
	if _, found := ss.lsRPC[HostPort]; !found {
		lsRPC, derr := rpc.DialHTTP("tcp", HostPort)
		if derr != nil {
			return false
		}
		ss.lsRPC[HostPort] = lsRPC // add rpc client
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
	ss.addRPC <- args.HostPort // wait for adding RPC
	<-ss.successReply
	// renew timestamp
	leaserecord := &LeaseRecord{timestamp: time.Now(), HostPort: args.HostPort}
	addPack := &AddPack{Key: args.Key, LeaseRecord: *leaserecord}
	ss.addRecord <- *addPack // wait for adding lease record
	<-ss.successReply

	Lease := &storagerpc.Lease{Granted: true, ValidSeconds: storagerpc.LeaseSeconds}
	return *Lease
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	if Value, found := ss.ItemMap[args.Key]; found {
		reply.Status = storagerpc.OK
		reply.Value = Value
		lease := ss.LeaseMaker(args)
		reply.Lease = lease
		return nil
	}
	reply.Status = storagerpc.KeyNotFound
	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	// assuem userID exists
	reply.Status = storagerpc.OK
	reply.Value = ss.ListMap[args.Key]
	lease := ss.LeaseMaker(args)
	reply.Lease = lease
	return nil
}

func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	// assume item in ItemMap
	if _, found := ss.ItemMap[args.Key]; found {
		delete(ss.ItemMap, args.Key)
		reply.Status = storagerpc.OK
		return nil
	}
	// not found
	reply.Status = storagerpc.ItemNotFound
	return nil
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	defer fmt.Println("Put")
	ss.ItemMap[args.Key] = args.Value
	reply.Status = storagerpc.OK
	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	// assume userID exists
	list := ss.ListMap[args.Key]
	i := FindPos(list, args.Value)
	if i != -1 { // already exists
		reply.Status = storagerpc.ItemExists
		return nil
	}
	list = append(list, args.Value)
	ss.ListMap[args.Key] = list
	reply.Status = storagerpc.OK
	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	// assume userID exists
	list := ss.ListMap[args.Key]
	i := FindPos(list, args.Value)
	if i == -1 { // not found in slice
		reply.Status = storagerpc.ItemNotFound
		return nil
	}
	list = append(list[:i], list[i+1:]...)
	ss.ListMap[args.Key] = list
	reply.Status = storagerpc.OK
	return nil
}

func (ss *storageServer) LeaseCallBack(key string) bool {
	// lsRPC := ss.lsRPC[key]

	// args := &{storagerpc.RevokeLeaseArgs: key}
	// var reply storagerpc.RevokeLeaseReply

}

func FindPos(s []string, value string) int {
	for p, v := range s {
		if v == value {
			return p
		}
	}
	return -1
}
