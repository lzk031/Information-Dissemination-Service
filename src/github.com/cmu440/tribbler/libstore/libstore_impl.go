package libstore

import (
	"errors"
	"fmt"
	"net/rpc"
	"sort"
	"time"

	"github.com/cmu440/tribbler/rpc/storagerpc"
	// "github.com/cmu440/tribbler/util"
)

type libstore struct {
	lib         map[string]*rpc.Client // HostPort to serverRPC
	ServerNodes []storagerpc.Node
	MyHostPort  string

	Ready chan storagerpc.GetServersReply
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

// NewLibstore creates a new instance of a TribServer's libstore. masterServerHostPort
// is the master storage server's host:port. myHostPort is this Libstore's host:port
// (i.e. the callback address that the storage servers should use to send back
// notifications when leases are revoked).
//
// The mode argument is a debugging flag that determines how the Libstore should
// request/handle leases. If mode is Never, then the Libstore should never request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to false). If mode is Always, then the Libstore should always request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to true). If mode is Normal, then the Libstore should make its own
// decisions on whether or not a lease should be requested from the storage server,
// based on the requirements specified in the project PDF handout.  Note that the
// value of the mode flag may also determine whether or not the Libstore should
// register to receive RPCs from the storage servers.
//
// To register the Libstore to receive RPCs from the storage servers, the following
// line of code should suffice:
//
//     rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
//
// Note that unlike in the NewTribServer and NewStorageServer functions, there is no
// need to create a brand new HTTP handler to serve the requests (the Libstore may
// simply reuse the TribServer's HTTP handler since the two run in the same process).
func NewLibstore(masterServerHostPort, myHostPort string, mode LeaseMode) (Libstore, error) {
	Libstore := new(libstore)
	Libstore.lib = make(map[string]*rpc.Client)
	Libstore.MyHostPort = myHostPort
	Libstore.Ready = make(chan storagerpc.GetServersReply)

	// listen to master server
	lib, err := rpc.DialHTTP("tcp", masterServerHostPort)
	if err != nil {
		fmt.Println("Error: NewLibstore DialHTTP:", err)
		return nil, err
	}
	Libstore.lib[masterServerHostPort] = lib
	go Libstore.ContactMasterServer(masterServerHostPort)

	// block slave servers
	reply := <-Libstore.Ready
	if reply.Status != storagerpc.OK { // fail retry upto 5 times
		return nil, errors.New("Error: Master Storage Server is not ready")
	}
	// sort reply by NodeID
	Servers := NodeSlice(reply.Servers)
	sort.Sort(Servers)
	Libstore.ServerNodes = []storagerpc.Node(Servers)
	// create rpc to all storage servers
	if aerr := Libstore.AddStorageRPC(); aerr != nil {
		return nil, aerr
	}
	// rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
	return Libstore, nil
}

func (ls *libstore) AddStorageRPC() error {
	for _, node := range ls.ServerNodes {
		if _, found := ls.lib[node.HostPort]; !found {
			lib, err := rpc.DialHTTP("tcp", node.HostPort)
			if err != nil {
				return err
			}
			ls.lib[node.HostPort] = lib // add rpc client
		}
	}
	return nil
}

func (ls *libstore) ContactMasterServer(masterServerHostPort string) {
	lib := ls.lib[masterServerHostPort]
	args := &storagerpc.GetServersArgs{}
	var reply storagerpc.GetServersReply
	for i := 0; i < 6; i++ {
		_ = lib.Call("StorageServer.GetServers", args, &reply)
		if reply.Status != storagerpc.OK {
			time.Sleep(1 * time.Second)
		} else { // Status = OK
			fmt.Println(reply.Servers)
			ls.Ready <- reply
			return
		}
	}
	ls.Ready <- reply
	return
}

func (ls *libstore) Get(key string) (string, error) {
	// fmt.Println("Lib: Get")
	// defer fmt.Println("Lib: Get Done")
	lib := ls.FindRPC(key)
	args := &storagerpc.GetArgs{Key: key, WantLease: false, HostPort: ls.MyHostPort}
	var reply storagerpc.GetReply
	_ = lib.Call("StorageServer.Get", args, &reply)
	if reply.Status != storagerpc.OK {
		return "", errors.New("Error on lib:Get")
	}
	return reply.Value, nil
}

func (ls *libstore) FindRPC(key string) *rpc.Client {
	NodeNum := StoreHash(key)
	fmt.Println(NodeNum)
	for _, ServerNode := range ls.ServerNodes {
		if ServerNode.NodeID >= NodeNum {
			return ls.lib[ServerNode.HostPort]
		}
	}
	// if cannot find, choose the first node
	return ls.lib[ls.ServerNodes[0].HostPort]
}

func (ls *libstore) Put(key, value string) error {
	// fmt.Println("Lib: Put")
	// defer fmt.Println("Lib: Put Done")
	lib := ls.FindRPC(key)
	args := &storagerpc.PutArgs{Key: key, Value: value}
	var reply storagerpc.PutReply
	_ = lib.Call("StorageServer.Put", args, &reply)
	if reply.Status != storagerpc.OK {
		return errors.New("Error on lib:Put")
	}
	return nil
}

func (ls *libstore) Delete(key string) error {
	// fmt.Println("Lib: Delete")
	// defer fmt.Println("Lib: Delete Done")
	lib := ls.FindRPC(key)
	args := &storagerpc.DeleteArgs{Key: key}
	var reply storagerpc.DeleteReply
	_ = lib.Call("StorageServer.Delete", args, &reply)
	if reply.Status != storagerpc.OK {
		return errors.New("Error on lib:Delete")
	}
	return nil
}

func (ls *libstore) GetList(key string) ([]string, error) {
	// fmt.Println("Lib: GetList")
	// defer fmt.Println("Lib: GetList Done")
	lib := ls.FindRPC(key)
	args := &storagerpc.GetArgs{Key: key, WantLease: false, HostPort: ls.MyHostPort}
	var reply storagerpc.GetListReply
	_ = lib.Call("StorageServer.GetList", args, &reply)
	if reply.Status != storagerpc.OK {
		return nil, errors.New("Error on lib:GetList")
	}
	return reply.Value, nil
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	// fmt.Println("Lib: RemoveFromList")
	// defer fmt.Println("Lib: RemoveFromList Done")
	lib := ls.FindRPC(key)
	args := &storagerpc.PutArgs{Key: key, Value: removeItem}
	var reply storagerpc.PutReply
	_ = lib.Call("StorageServer.RemoveFromList", args, &reply)
	if reply.Status != storagerpc.OK {
		return errors.New("Error on lib:RemoveFromList")
	}
	return nil
}

func (ls *libstore) AppendToList(key, newItem string) error {
	// fmt.Println("Lib: AppendToList")
	// defer fmt.Println("Lib: AppendToList Done")
	lib := ls.FindRPC(key)
	args := &storagerpc.PutArgs{Key: key, Value: newItem}
	var reply storagerpc.PutReply
	_ = lib.Call("StorageServer.AppendToList", args, &reply)
	if reply.Status != storagerpc.OK {
		return errors.New("Error on lib:AppendToList")
	}
	return nil
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	return errors.New("not implemented")
}
