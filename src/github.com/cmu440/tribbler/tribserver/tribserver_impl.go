package tribserver

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/util"
	// "github.com/cmu440/tribbler/rpc/storagerpc"
	"github.com/cmu440/tribbler/rpc/tribrpc"
)

type tribServer struct {
	lib libstore.Libstore
}

type KeySliceForSort []KeyForSort

type KeyForSort struct {
	Posted  int64
	PostKey string
}

// NewTribServer creates, starts and returns a new TribServer. masterServerHostPort
// is the master storage server's host:port and port is this port number on which
// the TribServer should listen. A non-nil error should be returned if the TribServer
// could not be started.
//
// For hints on how to properly setup RPC, see the rpc/tribrpc package.
func NewTribServer(masterServerHostPort, myHostPort string) (TribServer, error) {

	tribServer := new(tribServer)

	// Create the server socket that will listen for incoming RPCs.
	listener, err := net.Listen("tcp", myHostPort)
	if err != nil {
		fmt.Println("Error on TribServer Listen", err)
		return nil, err
	}

	// Wrap the tribServer before registering it for RPC.
	if err := rpc.RegisterName("TribServer", tribrpc.Wrap(tribServer)); err != nil {
		fmt.Println("Error on TribServer RegisterName", err)
		return nil, err
	}

	// Setup the HTTP handler that will server incoming RPCs and
	// serve requests in a background goroutine.
	rpc.HandleHTTP()
	go http.Serve(listener, nil)

	// Create LibStoreServer
	// Lease Mode 0 - Never
	lib, _ := libstore.NewLibstore(masterServerHostPort, myHostPort, 0)
	fmt.Println(lib)
	tribServer.lib = lib

	return tribServer, nil

}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	fmt.Println("CreatUser")
	defer fmt.Println("CreatUser Done")
	UserKey := util.FormatUserKey(args.UserID)
	if err := ts.lib.Put(UserKey, "I will never be deleted!:D"); err != nil {
		reply.Status = tribrpc.Exists
		return nil
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	fmt.Println("AddSubscription")
	defer fmt.Println("AddSubscription Done")
	UserKey := util.FormatUserKey(args.UserID)
	TargetUserKey := util.FormatUserKey(args.UserID)
	// first check userID and TargetUserID
	if _, uerr := ts.lib.Get(UserKey); uerr != nil {
		reply.Status = tribrpc.NoSuchUser
		return uerr
	}
	if _, terr := ts.lib.Get(TargetUserKey); terr != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return terr
	}
	// if both keys exist in storage server
	SubListKey := util.FormatSubListKey(args.TargetUserID)
	if err := ts.lib.AppendToList(UserKey, SubListKey); err != nil {
		reply.Status = tribrpc.Exists
		return err
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	fmt.Println("RemoveSubscription")
	defer fmt.Println("RemoveSubscription Done")
	UserKey := util.FormatUserKey(args.UserID)
	TargetUserKey := util.FormatUserKey(args.UserID)
	// first check UserID and TargetUserID
	if _, uerr := ts.lib.Get(UserKey); uerr != nil {
		reply.Status = tribrpc.NoSuchUser
		return uerr
	}
	if _, terr := ts.lib.Get(TargetUserKey); terr != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return terr
	}
	// if both keys exist in storage server
	SubListKey := util.FormatSubListKey(args.TargetUserID)
	if uerr := ts.lib.RemoveFromList(UserKey, SubListKey); uerr != nil {
		reply.Status = tribrpc.NoSuchUser
		return uerr
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetSubscriptions(args *tribrpc.GetSubscriptionsArgs, reply *tribrpc.GetSubscriptionsReply) error {
	fmt.Println("GetSubscription")
	defer fmt.Println("GetSubscription Done")
	UserKey := util.FormatUserKey(args.UserID)
	// first check UserID
	if _, uerr := ts.lib.Get(UserKey); uerr != nil {
		reply.Status = tribrpc.NoSuchUser
		return uerr
	}
	// get subscribtion from storage server
	UserIDs, err := ts.lib.GetList(UserKey)
	if err != nil {
		return err
	}
	reply.UserIDs = UserIDs
	fmt.Println(UserIDs)
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	fmt.Println("PostTribble")
	defer fmt.Println("PostTribble Done")
	UserKey := util.FormatUserKey(args.UserID)
	// first check UserID
	if _, uerr := ts.lib.Get(UserKey); uerr != nil {
		reply.Status = tribrpc.NoSuchUser
		return uerr
	}
	// userID exists in storage server
	// Tribble
	tribble := &tribrpc.Tribble{UserID: args.UserID, Posted: time.Now(), Contents: args.Contents}
	tribBytes, merr := json.Marshal(*tribble)
	if merr != nil {
		return merr
	}

	// postkey
	PostKey := util.FormatPostKey(args.UserID, tribble.Posted.Unix())
	if err := ts.lib.Put(PostKey, string(tribBytes)); err != nil {
		reply.Status = tribrpc.Exists
		return nil
	}

	// Tribble list key
	TribListKey := util.FormatTribListKey(args.UserID)
	if err := ts.lib.AppendToList(TribListKey, PostKey); err != nil {
		reply.Status = tribrpc.Exists
		return err
	}
	reply.PostKey = PostKey
	fmt.Println(PostKey)
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) DeleteTribble(args *tribrpc.DeleteTribbleArgs, reply *tribrpc.DeleteTribbleReply) error {
	fmt.Println("DeleteTribble")
	defer fmt.Println("DeleteTribble Done")
	UserKey := util.FormatUserKey(args.UserID)
	// first check UserID
	if _, uerr := ts.lib.Get(UserKey); uerr != nil {
		reply.Status = tribrpc.NoSuchUser
		return uerr
	}
	// UserID exists
	// Delete Item Map
	if derr := ts.lib.Delete(args.PostKey); derr != nil {
		reply.Status = tribrpc.NoSuchPost
		return derr
	}
	// Remove from list
	TribListKey := util.FormatTribListKey(args.UserID)
	if uerr := ts.lib.RemoveFromList(TribListKey, args.PostKey); uerr != nil {
		reply.Status = tribrpc.NoSuchUser
		return uerr
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	fmt.Println("GetTribble")
	defer fmt.Println("GetTribble Done")
	UserKey := util.FormatUserKey(args.UserID)
	// first check UserID
	if _, uerr := ts.lib.Get(UserKey); uerr != nil {
		reply.Status = tribrpc.NoSuchUser
		return uerr
	}
	// UserID exists
	TribListKey := util.FormatTribListKey(args.UserID)
	PostKeySliceNaive, err := ts.lib.GetList(TribListKey)
	if err != nil {
		return err
	}
	// sort PostKeySlice
	PostKeySlice, _ := ts.OneHundredPostKey(PostKeySliceNaive)
	// fetch marshalled tribbles and then unmarshall
	tribbleSlice := make([]tribrpc.Tribble, len(PostKeySlice))
	for i, PostKey := range PostKeySlice {
		// fetch tribble by PostKey
		tribbleBytes, perr := ts.lib.Get(PostKey)
		if perr != nil {
			reply.Status = tribrpc.NoSuchPost
			return perr
		}
		// unmarshal
		var tribble tribrpc.Tribble
		_ = json.Unmarshal([]byte(tribbleBytes), &tribble)
		tribbleSlice[i] = tribble
	}
	reply.Tribbles = tribbleSlice
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	fmt.Println("GetTribbleBySubscription")
	defer fmt.Println("GetTribbleBySubscription Done")
	UserKey := util.FormatUserKey(args.UserID)
	// first check UserID
	if _, uerr := ts.lib.Get(UserKey); uerr != nil {
		reply.Status = tribrpc.NoSuchUser
		return uerr
	}
	// then retreive the subscription list
	UserIDs, err := ts.lib.GetList(UserKey)
	if err != nil {
		return err
	}
	// GetTribbles One by One
	PostKeySliceNaiveAppend := make([]string, 0)
	for _, UserID := range UserIDs {
		// parse UserID
		parts := strings.Split(UserID, ":")
		TribListKey := util.FormatTribListKey(parts[0])
		PostKeySliceNaive, err := ts.lib.GetList(TribListKey)
		if err != nil {
			return err
		}
		PostKeySliceNaiveAppend = append(PostKeySliceNaiveAppend, PostKeySliceNaive...)
	}
	// sort PostKeySliceNaiveAppendfmt.Println("PostKeySliceNaiveAppend")
	PostKeySlice, _ := ts.OneHundredPostKey(PostKeySliceNaiveAppend)
	// fetch marshalled tribbles and then unmarshall
	tribbleSlice := make([]tribrpc.Tribble, len(PostKeySlice))
	for i, PostKey := range PostKeySlice {
		// fetch tribble by PostKey
		tribbleBytes, perr := ts.lib.Get(PostKey)
		if perr != nil {
			reply.Status = tribrpc.NoSuchPost
			return perr
		}
		// unmarshal
		var tribble tribrpc.Tribble
		_ = json.Unmarshal([]byte(tribbleBytes), &tribble)
		tribbleSlice[i] = tribble
	}
	reply.Tribbles = tribbleSlice
	reply.Status = tribrpc.OK
	return nil
}

// OneHundeedPostKey sort the string slice by their timestamp and pick top 100 tibbles
// if input slice has less than 100 tribbles, then return all tribbles
func (ts *tribServer) OneHundredPostKey(KeySlice []string) (OneHundredKey []string, err error) {
	// sort the slice by timestamp
	KeySliceSort := ts.Sort(KeySlice)
	if len(KeySliceSort) > 100 {
		OneHundredKey = KeySliceSort[:100]
	} else {
		OneHundredKey = KeySliceSort
	}
	return OneHundredKey, nil
}

func (ts *tribServer) Sort(PostKeySlice []string) []string {
	KeySliceForSort := make(KeySliceForSort, len(PostKeySlice))
	KeySliceSort := make([]string, len(PostKeySlice))
	// extract Posted time to sort
	for i, postKey := range PostKeySlice {
		PostKeyParts := strings.Split(postKey, "_")
		posted, _ := strconv.ParseInt(PostKeyParts[1], 16, 64)
		keyForSort := &KeyForSort{Posted: posted, PostKey: postKey}
		KeySliceForSort[i] = *keyForSort
	}
	sort.Sort(KeySliceForSort)
	// get rid of Posted time
	for j, keyForSort2 := range KeySliceForSort {
		KeySliceSort[j] = keyForSort2.PostKey
	}
	return KeySliceSort
}

func (key KeySliceForSort) Len() int {
	return len(key)
}

func (key KeySliceForSort) Swap(i, j int) {
	key[i], key[j] = key[j], key[i]
	return
}

func (key KeySliceForSort) Less(i, j int) bool {
	return key[i].Posted > key[j].Posted // in desc order
}
