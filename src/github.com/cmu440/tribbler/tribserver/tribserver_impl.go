package tribserver

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/rpc"

	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/util"
	// "github.com/cmu440/tribbler/rpc/storagerpc"
	"github.com/cmu440/tribbler/rpc/tribrpc"
)

type tribServer struct {
	lib libstore.Libstore
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
	if err := ts.lib.Put(UserKey, "hello"); err != nil {
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
	return errors.New("not implemented")
}

func (ts *tribServer) DeleteTribble(args *tribrpc.DeleteTribbleArgs, reply *tribrpc.DeleteTribbleReply) error {
	return errors.New("not implemented")
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	return errors.New("not implemented")
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	return errors.New("not implemented")
}
