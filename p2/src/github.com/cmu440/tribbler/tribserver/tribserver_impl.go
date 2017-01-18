package tribserver

import (
	"encoding/json"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
	"github.com/cmu440/tribbler/util"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"time"
)

const TRIBBLES_LIMIT = 100

type tribServer struct {
	ls libstore.Libstore
}

// Interface for sorting tribbles
type SortableTribbles []tribrpc.Tribble

func (tribbles SortableTribbles) Len() int {
	return len(tribbles)
}
func (tribbles SortableTribbles) Swap(i, j int) {
	tribbles[i], tribbles[j] = tribbles[j], tribbles[i]
}
func (tribbles SortableTribbles) Less(i, j int) bool {
	t1 := tribbles[i].Posted
	t2 := tribbles[j].Posted
	// reverse chronological
	return t2.Before(t1)
}

// NewTribServer creates, starts and returns a new TribServer. masterServerHostPort
// is the master storage server's host:port and port is this port number on which
// the TribServer should listen. A non-nil error should be returned if the TribServer
// could not be started.
//
// For hints on how to properly setup RPC, see the rpc/tribrpc package.
func NewTribServer(masterServerHostPort, port string) (TribServer, error) {
	tribServer := new(tribServer)

	// create an instance of Libstore
	ls, err := libstore.NewLibstore(masterServerHostPort, port, libstore.Never)
	if err != nil {
		return nil, err
	}
	tribServer.ls = ls

	// Create the server socket that will listen for incoming RPCs.
	listener, err := net.Listen("tcp", port)
	if err != nil {
		return nil, err
	}

	// Wrap the tribServer before registering it for RPC.
	err = rpc.RegisterName("TribServer", tribrpc.Wrap(tribServer))
	if err != nil {
		return nil, err
	}

	// Setup the HTTP handler that will server incoming RPCs and
	// serve requests in a background goroutine.
	rpc.HandleHTTP()
	go http.Serve(listener, nil)

	return tribServer, nil
}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {

	reply.Status = tribrpc.Exists
	_, err := ts.ls.Get(util.FormatUserKey(args.UserID))

	//libstore will return error if key doesnt exist
	if err != nil {
		reply.Status = tribrpc.OK
		ts.ls.Put(util.FormatUserKey(args.UserID), "")
	}
	return nil
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	// check if args.UserID has been created
	_, err := ts.ls.Get(util.FormatUserKey(args.UserID))
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	// check if args.TargetUserID has been created
	_, err = ts.ls.Get(util.FormatUserKey(args.TargetUserID))
	if err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}

	// add targetUserID to FormatSubListKey list
	key := util.FormatSubListKey(args.UserID)
	err = ts.ls.AppendToList(key, args.TargetUserID)
	if err != nil {
		//assume only error for now
		reply.Status = tribrpc.Exists
		return nil
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	// check if args.UserID has been created
	// if not, reply.Status = tribrpc.NoSuchUser
	_, err := ts.ls.Get(util.FormatUserKey(args.UserID))
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	// check if args.TargetUserID has been created
	// if not, reply.Status = tribrpc.NoSuchTargetUser
	_, err = ts.ls.Get(util.FormatUserKey(args.TargetUserID))
	if err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}

	// removeFromList
	key := util.FormatSubListKey(args.UserID)
	err = ts.ls.RemoveFromList(key, args.TargetUserID)
	if err != nil {
		//assume only error for now
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetFriends(args *tribrpc.GetFriendsArgs, reply *tribrpc.GetFriendsReply) error {
	// check if args.UserID has been created
	// if not, reply.Status = tribrpc.NoSuchUser
	_, err := ts.ls.Get(util.FormatUserKey(args.UserID))
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	myUserID := args.UserID
	key := util.FormatSubListKey(myUserID)
	// get our subscriptions
	mySubs, err := ts.ls.GetList(key)
	if err != nil {
		if err.Error() != libstore.KEYNOTFOUND {
			return err
		}
	}
	friends := make([]string, 0)
	// go through each sub
	for _, mySub := range mySubs {
		key = util.FormatSubListKey(mySub)
		theirSubs, err := ts.ls.GetList(key)
		if err != nil {
			if err.Error() != libstore.KEYNOTFOUND {
				return err
			}
			continue
		}
		// check if they are subscribed to us
		for _, theirSub := range theirSubs {
			if theirSub == myUserID {
				friends = append(friends, mySub)
				break
			}
		}
	}
	reply.UserIDs = friends
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	// check if args.UserID has been created
	// if not, reply.Status = tribrpc.NoSuchUser
	_, err := ts.ls.Get(util.FormatUserKey(args.UserID))
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	timeNow := time.Now()
	tribbleKey := util.FormatPostKey(args.UserID, timeNow.UnixNano())
	// not checking collisions due to low probability.

	// key for list of tribble keys
	tribbleListKey := util.FormatTribListKey(args.UserID)
	err = ts.ls.AppendToList(tribbleListKey, tribbleKey)
	if err != nil {
		return err
	}

	// create tribble and store as json-marshalled string
	newTrib := tribrpc.Tribble{UserID: args.UserID, Posted: timeNow,
		Contents: args.Contents}
	newTribM, _ := json.Marshal(newTrib)
	err = ts.ls.Put(tribbleKey, string(newTribM))
	if err != nil {
		return err
	}

	reply.Status = tribrpc.OK
	reply.PostKey = tribbleKey
	return nil
}

func (ts *tribServer) DeleteTribble(args *tribrpc.DeleteTribbleArgs, reply *tribrpc.DeleteTribbleReply) error {
	tribbleKey := args.PostKey

	tribListKey := util.FormatTribListKey(args.UserID)

	err := ts.ls.RemoveFromList(tribListKey, tribbleKey)
	if err != nil {
		switch err.Error() {
		case libstore.ITEMNOTFOUND:
			reply.Status = tribrpc.NoSuchPost
		case libstore.KEYNOTFOUND:
			reply.Status = tribrpc.NoSuchUser
		}
		return nil
	}

	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	userID := args.UserID
	// check if args.UserID has been created
	_, err := ts.ls.Get(util.FormatUserKey(userID))
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	//get list of user's tribbles
	key := util.FormatTribListKey(userID)
	tribKeys, err := ts.ls.GetList(key)
	if err != nil {
		if err.Error() != libstore.KEYNOTFOUND {
			return err
		}
	}
	result := ts.pullTribbles(tribKeys)

	sort.Sort(SortableTribbles(result))

	reply.Status = tribrpc.OK
	reply.Tribbles = result
	return nil
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	userID := args.UserID

	// check if args.UserID has been created
	_, err := ts.ls.Get(util.FormatUserKey(userID))
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	// get subscriptions
	subs, err := ts.ls.GetList(util.FormatSubListKey(userID))
	if err != nil {
		if err.Error() != libstore.KEYNOTFOUND {
			return err
		}
		reply.Tribbles = make([]tribrpc.Tribble, 0)
		reply.Status = tribrpc.OK
		return nil
	}

	allTribbles := make([]tribrpc.Tribble, 0)
	// get each sub's tribbles
	for _, subID := range subs {
		key := util.FormatTribListKey(subID)
		tribKeys, err := ts.ls.GetList(key)
		// no error => has some tribbles
		if err == nil {
			tribblesFromUser := ts.pullTribbles(tribKeys)
			//append all
			allTribbles = append(allTribbles, tribblesFromUser...)
		}
	}

	sort.Sort(SortableTribbles(allTribbles))

	//client needs just newest few.
	if len(allTribbles) > TRIBBLES_LIMIT {
		allTribbles = allTribbles[:TRIBBLES_LIMIT]
	}
	reply.Tribbles = allTribbles
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) pullTribbles(tribKeys []string) []tribrpc.Tribble {
	result := make([]tribrpc.Tribble, 0)
	counter := 0
	var currTrib tribrpc.Tribble
	for i := len(tribKeys) - 1; i >= 0 && counter < TRIBBLES_LIMIT; i-- {
		tribKey := tribKeys[i]
		tribContents, err := ts.ls.Get(tribKey)
		if err != nil {
			continue
		}

		json.Unmarshal([]byte(tribContents), &currTrib)

		result = append(result, tribrpc.Tribble{
			UserID:   currTrib.UserID,
			Posted:   currTrib.Posted,
			Contents: currTrib.Contents,
		})
		counter++
	}
	return result
}
