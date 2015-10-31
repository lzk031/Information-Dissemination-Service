package libstore

import (
	"errors"
	"net/rpc"

	"github.com/cmu440/tribbler/rpc/storagerpc"
	// "github.com/cmu440/tribbler/util"
)

type libstore struct {
	lib      *rpc.Client
	HostPort string
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
	// this is similar to client
	lib, err := rpc.DialHTTP("tcp", masterServerHostPort)
	if err != nil {
		// fmt.Println("Error: NewLibstore DialHTTP:", err)
		return nil, err
	}
	// rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
	return &libstore{lib: lib, HostPort: myHostPort}, nil
}

func (ls *libstore) Get(key string) (string, error) {
	// fmt.Println("Lib: Get")
	// defer fmt.Println("Lib: Get Done")
	args := &storagerpc.GetArgs{Key: key, WantLease: false, HostPort: ls.HostPort}
	var reply storagerpc.GetReply
	_ = ls.lib.Call("StorageServer.Get", args, &reply)
	if reply.Status != storagerpc.OK {
		return "", errors.New("Error on lib:Get")
	}
	return reply.Value, nil
}

func (ls *libstore) Put(key, value string) error {
	// fmt.Println("Lib: Put")
	// defer fmt.Println("Lib: Put Done")
	args := &storagerpc.PutArgs{Key: key, Value: value}
	var reply storagerpc.PutReply
	_ = ls.lib.Call("StorageServer.Put", args, &reply)
	if reply.Status != storagerpc.OK {
		return errors.New("Error on lib:Put")
	}
	return nil
}

func (ls *libstore) Delete(key string) error {
	// fmt.Println("Lib: Delete")
	// defer fmt.Println("Lib: Delete Done")
	args := &storagerpc.DeleteArgs{Key: key}
	var reply storagerpc.DeleteReply
	_ = ls.lib.Call("StorageServer.Delete", args, &reply)
	if reply.Status != storagerpc.OK {
		return errors.New("Error on lib:Delete")
	}
	return nil
}

func (ls *libstore) GetList(key string) ([]string, error) {
	// fmt.Println("Lib: GetList")
	// defer fmt.Println("Lib: GetList Done")
	args := &storagerpc.GetArgs{Key: key, WantLease: false, HostPort: ls.HostPort}
	var reply storagerpc.GetListReply
	_ = ls.lib.Call("StorageServer.GetList", args, &reply)
	if reply.Status != storagerpc.OK {
		return nil, errors.New("Error on lib:GetList")
	}
	return reply.Value, nil
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	// fmt.Println("Lib: RemoveFromList")
	// defer fmt.Println("Lib: RemoveFromList Done")
	args := &storagerpc.PutArgs{Key: key, Value: removeItem}
	var reply storagerpc.PutReply
	_ = ls.lib.Call("StorageServer.RemoveFromList", args, &reply)
	if reply.Status != storagerpc.OK {
		return errors.New("Error on lib:RemoveFromList")
	}
	return nil
}

func (ls *libstore) AppendToList(key, newItem string) error {
	// fmt.Println("Lib: AppendToList")
	// defer fmt.Println("Lib: AppendToList Done")
	args := &storagerpc.PutArgs{Key: key, Value: newItem}
	var reply storagerpc.PutReply
	_ = ls.lib.Call("StorageServer.AppendToList", args, &reply)
	if reply.Status != storagerpc.OK {
		return errors.New("Error on lib:AppendToList")
	}
	return nil
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	return errors.New("not implemented")
}
