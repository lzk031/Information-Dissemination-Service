package storageserver

import (
	// "container/list"

	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"time"

	"github.com/cmu440/tribbler/rpc/storagerpc"
)

type storageServer struct {
	ItemMap  map[string]string   // PostKey to value
	ListMap  map[string][]string // key defined in util
	numNodes int                 // number of storage servers expected
	// for slave servers
	Ready chan bool

	// for master servers
	ServerNodes []storagerpc.Node
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

	storageServer := new(storageServer)
	storageServer.ItemMap = make(map[string]string)
	storageServer.ListMap = make(map[string][]string)
	storageServer.numNodes = numNodes

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
		fmt.Println("Error: NewLibstore DialHTTP:", err)
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
	return storageServer, nil
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
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
		reply.Status = storagerpc.OK
		reply.Servers = ss.ServerNodes
		return nil
	}
	reply.Status = storagerpc.NotReady
	return nil
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	if len(ss.ServerNodes) == ss.numNodes { // all servers available
		reply.Status = storagerpc.OK
		reply.Servers = ss.ServerNodes
		return nil
	}
	reply.Status = storagerpc.NotReady
	reply.Servers = ss.ServerNodes
	return nil
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	if Value, found := ss.ItemMap[args.Key]; found {
		reply.Status = storagerpc.OK
		reply.Value = Value
		Lease := &storagerpc.Lease{Granted: false, ValidSeconds: 5}
		reply.Lease = *Lease
		return nil
	}
	reply.Status = storagerpc.KeyNotFound
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

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	// assuem userID exists
	reply.Status = storagerpc.OK
	reply.Value = ss.ListMap[args.Key]
	Lease := &storagerpc.Lease{Granted: false, ValidSeconds: 5}
	reply.Lease = *Lease
	return nil
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {

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

func FindPos(s []string, value string) int {
	for p, v := range s {
		if v == value {
			return p
		}
	}
	return -1
}
