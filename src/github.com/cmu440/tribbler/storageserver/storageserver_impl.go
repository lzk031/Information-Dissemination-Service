package storageserver

import (
	// "container/list"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"strconv"

	"github.com/cmu440/tribbler/rpc/storagerpc"
)

type storageServer struct {
	ItemMap map[string]string   // PostKey to value
	ListMap map[string][]string // key defined in util
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

	hostPort := net.JoinHostPort("localhost", strconv.Itoa(port))
	// for checkpoint, ignore masterServerHostPort first, but need to implement in the future
	// Create the server socket that will listen for incoming RPCs.
	listener, err := net.Listen("tcp", hostPort)
	if err != nil {
		fmt.Println("Error on StorageServer Listen", err)
		return nil, err
	}

	// Wrap the Server before registering it for RPC.
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
	return errors.New("not implemented")
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	return errors.New("not implemented")
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
	return errors.New("Key Not Found")
}

func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	// assuem userID exists
	fmt.Println("GET LIST")
	reply.Status = storagerpc.OK
	reply.Value = ss.ListMap[args.Key]
	Lease := &storagerpc.Lease{Granted: false, ValidSeconds: 5}
	reply.Lease = *Lease
	return nil
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	// check if exists first
	if _, found := ss.ItemMap[args.Key]; found {
		reply.Status = storagerpc.ItemExists
		return errors.New("Data already exists")
	}
	// does not exist
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
		return errors.New("Item Exists")
	}
	list = append(list, args.Value)
	ss.ListMap[args.Key] = list
	// ss.ListMap[args.Key] = append(ss.ListMap[args.Key], args.Value)
	reply.Status = storagerpc.OK
	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	// assume userID exists
	list := ss.ListMap[args.Key]
	i := FindPos(list, args.Value)
	if i == -1 { // not found in slice
		reply.Status = storagerpc.ItemNotFound
		return errors.New("Item Not Found")
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
