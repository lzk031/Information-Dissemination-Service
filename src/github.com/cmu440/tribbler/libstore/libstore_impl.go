package libstore

import (
	"errors"
	"fmt"
	"net/rpc"
	"sort"
	"time"

	"github.com/cmu440/tribbler/rpc/librpc"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	// "github.com/cmu440/tribbler/util"
)

type CacheStatus int

// cache constants
const (
	Found     = iota + 1 // item found in cache
	WantLease            // item not in cache and want lease
	SendQuery            // item not in cache but does not want lease
)

type libstore struct {
	ssRPC       map[string]*rpc.Client // rpc client to storage server
	ServerNodes []storagerpc.Node
	MyHostPort  string

	// critical section
	Cache     map[string]cacheitem
	KeyRecord map[string][]time.Time

	Ready           chan storagerpc.GetServersReply
	checkCache      chan string
	checkCacheReply chan cacheitem
	revokeCache     chan string
	// revokeCacheReply chan bool
	// expireCache      chan string
	addCache chan addcachePack
	// addCacheReply chan bool
	CacheReply chan bool
}

type addcachePack struct {
	Key  string
	Item cacheitem
}

type cacheitem struct {
	Status CacheStatus
	Item   interface{}
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
	ls := new(libstore)
	ls.MyHostPort = myHostPort
	ls.ssRPC = make(map[string]*rpc.Client)
	ls.Cache = make(map[string]cacheitem)
	ls.KeyRecord = make(map[string][]time.Time)

	ls.Ready = make(chan storagerpc.GetServersReply)
	ls.checkCache = make(chan string)
	ls.checkCacheReply = make(chan cacheitem)
	ls.revokeCache = make(chan string)
	// ls.revokeCacheReply = make(chan bool)
	ls.addCache = make(chan addcachePack)
	ls.CacheReply = make(chan bool)

	// listen to master server
	ssRPC, err := rpc.DialHTTP("tcp", masterServerHostPort)
	if err != nil {
		fmt.Println("Error: NewLibstore DialHTTP:", err)
		return nil, err
	}
	ls.ssRPC[masterServerHostPort] = ssRPC
	go ls.ContactMasterServer(masterServerHostPort)

	// block slave servers
	reply := <-ls.Ready
	if reply.Status != storagerpc.OK { // fail retry upto 5 times
		return nil, errors.New("Error: Master Storage Server is not ready")
	}
	// sort reply by NodeID
	Servers := NodeSlice(reply.Servers)
	sort.Sort(Servers)
	ls.ServerNodes = []storagerpc.Node(Servers)
	// create rpc to all storage servers
	if aerr := ls.AddStorageRPC(); aerr != nil {
		return nil, aerr
	}
	go ls.CacheHandler()

	// register rpc to receive call backs from storage servers
	// listener, err := net.Listen("tcp", myHostPort)
	// if err != nil {
	// 	fmt.Println("Error on LibServer Listen", err)
	// 	return nil, err
	// }
	if rerr := rpc.RegisterName("LeaseCallbacks", librpc.Wrap(ls)); rerr != nil {
		fmt.Println("Error on LibServer RegisterName", err)
		return nil, err
	}
	// rpc.HandleHTTP()
	// go http.Serve(listener, nil)

	return ls, nil
}

func (ls *libstore) CacheHandler() {
	for {
		select {
		case key := <-ls.checkCache:
			CacheItem := ls.CheckCache(key)
			ls.checkCacheReply <- CacheItem
		case key := <-ls.revokeCache:
			delete(ls.Cache, key)
			ls.CacheReply <- true
		case pack := <-ls.addCache:
			ls.Cache[pack.Key] = pack.Item
			ls.CacheReply <- true
		}
	}
}

func (ls *libstore) CheckCache(key string) cacheitem {
	item, found := ls.Cache[key]
	if found { // item already in cache
		return item
	}
	status := ls.CheckRecord(key)
	ItemReturn := &cacheitem{Status: status}
	// ItemReturn = &cacheitem{Status: status, Item: item}
	return *ItemReturn
}

func (ls *libstore) CheckRecord(key string) CacheStatus {
	record, found := ls.KeyRecord[key]
	if !found {
		record = make([]time.Time, 0)
	}
	if len(record) < storagerpc.QueryCacheThresh {
		record = append(record, time.Now())
		ls.KeyRecord[key] = record // update record
		return SendQuery
	}
	// check timestamp
	timestamp := record[len(record)-storagerpc.QueryCacheThresh]
	record = append(record, time.Now())
	ls.KeyRecord[key] = record[(len(record) - storagerpc.QueryCacheThresh):] // update record
	duration := -timestamp.Sub(time.Now())
	if duration < storagerpc.QueryCacheSeconds { // want lease
		return WantLease
	}
	return SendQuery
}

func (ls *libstore) ContactMasterServer(masterServerHostPort string) {
	ssRPC := ls.ssRPC[masterServerHostPort]
	args := &storagerpc.GetServersArgs{}
	var reply storagerpc.GetServersReply
	for i := 0; i < 6; i++ { // retry upto 5 times
		_ = ssRPC.Call("StorageServer.GetServers", args, &reply)
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

func (ls *libstore) AddStorageRPC() error {
	for _, node := range ls.ServerNodes {
		if _, found := ls.ssRPC[node.HostPort]; !found {
			ssRPC, err := rpc.DialHTTP("tcp", node.HostPort)
			if err != nil {
				return err
			}
			ls.ssRPC[node.HostPort] = ssRPC // add rpc client
		}
	}
	return nil
}

func (ls *libstore) FindRPC(key string) *rpc.Client {
	NodeNum := StoreHash(key)
	// fmt.Println(NodeNum)
	for _, ServerNode := range ls.ServerNodes {
		if ServerNode.NodeID >= NodeNum {
			return ls.ssRPC[ServerNode.HostPort]
		}
	}
	// if cannot find, choose the first node
	return ls.ssRPC[ls.ServerNodes[0].HostPort]
}

func (ls *libstore) Get(key string) (string, error) {
	// fmt.Println("Lib: Get")
	// defer fmt.Println("Lib: Get Done")
	wantlease := false
	ls.checkCache <- key
	item := <-ls.checkCacheReply
	switch item.Status {
	case Found:
		return item.Item.(string), nil
	case WantLease:
		wantlease = true
	}
	ssRPC := ls.FindRPC(key)
	args := &storagerpc.GetArgs{Key: key, WantLease: wantlease, HostPort: ls.MyHostPort}
	var reply storagerpc.GetReply
	_ = ssRPC.Call("StorageServer.Get", args, &reply)
	if reply.Status != storagerpc.OK {
		return "", errors.New("Error on Lib:Get")
	}
	if reply.Lease.Granted {
		item := &cacheitem{Status: Found, Item: reply.Value}
		pack := &addcachePack{Key: key, Item: *item}
		ls.addCache <- *pack
		<-ls.CacheReply
		go ls.ExpireCache(reply.Lease, key)
	}
	return reply.Value, nil
}

func (ls *libstore) Put(key, value string) error {
	// fmt.Println("Lib: Put")
	// defer fmt.Println("Lib: Put Done")
	ssRPC := ls.FindRPC(key)
	args := &storagerpc.PutArgs{Key: key, Value: value}
	var reply storagerpc.PutReply
	_ = ssRPC.Call("StorageServer.Put", args, &reply)
	if reply.Status != storagerpc.OK {
		return errors.New("Error on Lib:Put")
	}
	return nil
}

func (ls *libstore) Delete(key string) error {
	// fmt.Println("Lib: Delete")
	// defer fmt.Println("Lib: Delete Done")
	ssRPC := ls.FindRPC(key)
	args := &storagerpc.DeleteArgs{Key: key}
	var reply storagerpc.DeleteReply
	_ = ssRPC.Call("StorageServer.Delete", args, &reply)
	if reply.Status != storagerpc.OK {
		return errors.New("Error on Lib:Delete")
	}
	return nil
}

func (ls *libstore) GetList(key string) ([]string, error) {
	// fmt.Println("Lib: GetList")
	// defer fmt.Println("Lib: GetList Done")
	wantlease := false
	ls.checkCache <- key
	item := <-ls.checkCacheReply
	switch item.Status {
	case Found:
		return item.Item.([]string), nil
	case WantLease:
		wantlease = true
	}
	ssRPC := ls.FindRPC(key)
	args := &storagerpc.GetArgs{Key: key, WantLease: wantlease, HostPort: ls.MyHostPort}
	var reply storagerpc.GetListReply
	_ = ssRPC.Call("StorageServer.GetList", args, &reply)
	if reply.Status != storagerpc.OK {
		return nil, errors.New("Error on Lib:GetList")
	}
	if reply.Lease.Granted {
		item := &cacheitem{Status: Found, Item: reply.Value}
		pack := &addcachePack{Key: key, Item: *item}
		ls.addCache <- *pack
		<-ls.CacheReply
		go ls.ExpireCache(reply.Lease, key)
	}
	return reply.Value, nil
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	// fmt.Println("Lib: RemoveFromList")
	// defer fmt.Println("Lib: RemoveFromList Done")
	ssRPC := ls.FindRPC(key)
	args := &storagerpc.PutArgs{Key: key, Value: removeItem}
	var reply storagerpc.PutReply
	_ = ssRPC.Call("StorageServer.RemoveFromList", args, &reply)
	if reply.Status != storagerpc.OK {
		return errors.New("Error on Lib:RemoveFromList")
	}
	return nil
}

func (ls *libstore) AppendToList(key, newItem string) error {
	// fmt.Println("Lib: AppendToList")
	// defer fmt.Println("Lib: AppendToList Done")
	ssRPC := ls.FindRPC(key)
	args := &storagerpc.PutArgs{Key: key, Value: newItem}
	var reply storagerpc.PutReply
	_ = ssRPC.Call("StorageServer.AppendToList", args, &reply)
	if reply.Status != storagerpc.OK {
		return errors.New("Error on Lib:AppendToList")
	}
	return nil
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	ls.revokeCache <- args.Key
	<-ls.CacheReply
	// key not found ?
	reply.Status = storagerpc.OK
	return nil
}

func (ls *libstore) ExpireCache(lease storagerpc.Lease, key string) {
	<-time.After(time.Duration(lease.ValidSeconds) * time.Second)
	ls.revokeCache <- key
	<-ls.CacheReply
}
