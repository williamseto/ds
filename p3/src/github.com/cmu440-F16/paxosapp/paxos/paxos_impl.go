package paxos

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/cmu440-F16/paxosapp/rpc/paxosrpc"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

var PROPOSE_TIMEOUT = 15 * time.Second

type paxosNode struct {
	allNodes        map[int]*rpc.Client
	lastPropNum     map[string]int
	myID            int
	highestNForKey  map[string]int
	alreadyAccepted map[string]aaTuple
	storage         map[string]interface{}

	highestNForKeyMux  sync.Mutex
	storageMux         sync.Mutex
	alreadyAcceptedMux sync.Mutex
	lastPropNumMux     sync.Mutex
}

type aaTuple struct {
	propNum int
	value   interface{}
}

// Desc:
// NewPaxosNode creates a new PaxosNode. This function should return only when
// all nodes have joined the ring, and should return a non-nil error if this node
// could not be started in spite of dialing any other nodes numRetries times.
//
// Params:
// myHostPort: the hostport string of this new node. We use tcp in this project.
//			   	Note: Please listen to this port rather than hostMap[srvId]
// hostMap: a map from all node IDs to their hostports.
//				Note: Please connect to hostMap[srvId] rather than myHostPort
//				when this node try to make rpc call to itself.
// numNodes: the number of nodes in the ring
// numRetries: if we can't connect with some nodes in hostMap after numRetries attempts, an error should be returned
// replace: a flag which indicates whether this node is a replacement for a node which failed.
func NewPaxosNode(myHostPort string, hostMap map[int]string, numNodes, srvId, numRetries int, replace bool) (PaxosNode, error) {

	paxosNode := &paxosNode{
		allNodes:        make(map[int]*rpc.Client),
		lastPropNum:     make(map[string]int),
		myID:            srvId,
		highestNForKey:  make(map[string]int),
		alreadyAccepted: make(map[string]aaTuple),
		storage:         make(map[string]interface{}),
	}
	// Wrap the storageServer before registering it for RPC.
	err := rpc.RegisterName("PaxosNode", paxosrpc.Wrap(paxosNode))
	if err != nil {
		return nil, err
	}

	// Setup the HTTP handler that will server incoming RPCs
	rpc.HandleHTTP()

	// Create the server socket that will listen for incoming RPCs.
	listener, err := net.Listen("tcp", myHostPort)
	if err != nil {
		return nil, err
	}

	// serve requests in a background goroutine.
	go http.Serve(listener, nil)

	// dial each node in hostMap up to numRetries times
	for nodeID, hostPort := range hostMap {
		cli, err := rpc.DialHTTP("tcp", hostPort)
		i := 1
		for ; err != nil && i < numRetries; i++ {
			cli, err = rpc.DialHTTP("tcp", hostPort)
			// sleep for 1 second
			time.Sleep(time.Duration(1) * time.Second)
		}
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Couldn't connect to nodeId=%d, port=%s, from=%s", nodeID, hostPort, myHostPort))
		}
		paxosNode.allNodes[nodeID] = cli
	}

	// notify all other nodes of existence
	if replace {
		replaceArgs := &paxosrpc.ReplaceServerArgs{
			SrvID:    srvId,
			Hostport: myHostPort,
		}
		var replaceReply paxosrpc.ReplaceServerReply
		for _, cli := range paxosNode.allNodes {
			cli.Go("PaxosNode.RecvReplaceServer", replaceArgs, &replaceReply, nil)
		}

		catchupArgs := &paxosrpc.ReplaceCatchupArgs{}
		var catchupReply paxosrpc.ReplaceCatchupReply
		var tempMap map[string]interface{}
		for nodeID, cli := range paxosNode.allNodes {
			if nodeID != srvId {
				err := cli.Call("PaxosNode.RecvReplaceCatchup", catchupArgs, &catchupReply)
				if err == nil {
					buf := bytes.NewBuffer(catchupReply.Data)
					decoder := gob.NewDecoder(buf)
					decoder.Decode(&tempMap)
					// copy keys from each map
					for k, v := range tempMap {
						paxosNode.storage[k] = v
					}
				}
			}
		}
	}
	return paxosNode, nil
}

// Desc:
// GetNextProposalNumber generates a proposal number which will be passed to
// Propose. Proposal numbers should not repeat for a key, and for a particular
// <node, key> pair, they should be strictly increasing.
//
// Params:
// args: the key to propose
// reply: the next proposal number for the given key
func (pn *paxosNode) GetNextProposalNumber(args *paxosrpc.ProposalNumberArgs, reply *paxosrpc.ProposalNumberReply) error {
	// 0 if first time or lastN+1
	lastN, found := pn.getFromLastPropNum(args.Key)

	if !found {
		pn.putToLastPropNum(args.Key, 0)
	} else {
		pn.putToLastPropNum(args.Key, lastN+1)
	}

	// now construct a unique int based on nodeID to reply
	buf := new(bytes.Buffer)
	var final uint32
	var data = []interface{}{
		uint8(0),
		uint16(pn.lastPropNum[args.Key]),
		uint8(pn.myID),
	}
	for _, v := range data {
		binary.Write(buf, binary.BigEndian, v)
	}
	r := bytes.NewReader(buf.Bytes())
	binary.Read(r, binary.BigEndian, &final)

	reply.N = int(final)

	return nil
}

// Desc:
// Propose initializes proposing a value for a key, and replies with the
// value that was committed for that key. Propose should not return until
// a value has been committed, or PROPOSE_TIMEOUT seconds have passed.
//
// Params:
// args: the key, value pair to propose together with the proposal number returned by GetNextProposalNumber
// reply: value that was actually committed for the given key
func (pn *paxosNode) Propose(args *paxosrpc.ProposeArgs, reply *paxosrpc.ProposeReply) error {

	timer := time.NewTimer(PROPOSE_TIMEOUT)
	prepareChan := make(chan *rpc.Call, len(pn.allNodes))
	acceptChan := make(chan *rpc.Call, len(pn.allNodes))
	commitChan := make(chan *rpc.Call, len(pn.allNodes))
	totalResponseCount := 0
	errCount := 0

	// input for RecvPrepare
	prepareArgs := &paxosrpc.PrepareArgs{
		Key:         args.Key,
		N:           args.N,
		RequesterId: pn.myID,
	}

	// Send RecvPrepare RPC to all nodes
	pReplies := make([]paxosrpc.PrepareReply, 0)
	for _, rpcClient := range pn.allNodes {
		var prepareReply paxosrpc.PrepareReply
		rpcClient.Go("PaxosNode.RecvPrepare", prepareArgs, &prepareReply, prepareChan)
	}

	for {
		select {
		case prepareCall := <-prepareChan:
			if prepareCall.Error == nil {
				reply, _ := prepareCall.Reply.(*paxosrpc.PrepareReply)
				if reply.Status == paxosrpc.Reject {
					return errors.New("Received Reject in Prepare")
				} else if reply.Status == paxosrpc.OK {
					pReplies = append(pReplies, *reply)
				}
			} else {
				errCount++
			}
			totalResponseCount++
		case <-timer.C:
			// return error if not done in 15 seconds
			return errors.New("Timed out waiting for RecvPrepare")
		}
		// stop waiting if we got majority
		if len(pReplies) > len(pn.allNodes)/2 {
			break
		} else if totalResponseCount == len(pn.allNodes) {
			// didn't get majority and it's already over
			return errors.New("Couldn't achieve majority in Prepare")
		}
	}

	// Use the highest n out of those that came in prepare responses
	nForAccept := -1
	var vForAccept interface{}
	for _, pReply := range pReplies {
		if pReply.N_a > nForAccept {
			nForAccept = pReply.N_a
			vForAccept = pReply.V_a
		}
	}
	// use your own if there were none in responses
	if nForAccept == -1 {
		nForAccept = args.N
		vForAccept = args.V
	}

	if args.N > nForAccept {
		nForAccept = args.N
	}

	// Send RecvAccept RPC
	// input for RecvAccept
	acceptArgs := &paxosrpc.AcceptArgs{
		Key:         args.Key,
		N:           nForAccept,
		RequesterId: pn.myID,
		V:           vForAccept,
	}
	aReplies := make([]paxosrpc.AcceptReply, 0)
	for _, rpcClient := range pn.allNodes {
		var acceptReply paxosrpc.AcceptReply
		rpcClient.Go("PaxosNode.RecvAccept", acceptArgs, &acceptReply, acceptChan)
	}

	errCount = 0
	totalResponseCount = 0
	for {
		select {
		case acceptCall := <-acceptChan:
			if acceptCall.Error == nil {
				reply, _ := acceptCall.Reply.(*paxosrpc.AcceptReply)
				if reply.Status == paxosrpc.Reject {
					return errors.New("Received Reject in Prepare")
				} else if reply.Status == paxosrpc.OK {
					aReplies = append(aReplies, *reply)
				}
			} else {
				errCount++
			}
			totalResponseCount++
		case <-timer.C:
			// return error if not done in 15 seconds
			return errors.New("Timed out waiting for RecvAccept")
		}
		// stop waiting if we got majority
		if len(aReplies) > len(pn.allNodes)/2 {
			break
		} else if totalResponseCount == len(pn.allNodes) {
			// didn't get majority and it's already over
			return errors.New("Couldn't achieve majority in Prepare")
		}
	}

	// Send RecvCommit RPC
	commitArgs := &paxosrpc.CommitArgs{
		Key:         args.Key,
		V:           vForAccept,
		RequesterId: pn.myID,
	}
	for _, rpcClient := range pn.allNodes {
		var commitReply paxosrpc.CommitReply
		rpcClient.Go("PaxosNode.RecvCommit", commitArgs, &commitReply, commitChan)
	}

	commitSuccessCount := 0
	commitTotalCount := 0
	for {
		select {
		case commitCall := <-commitChan:
			if commitCall.Error == nil {
				commitSuccessCount++
			}
			commitTotalCount++
		case <-timer.C:
			// return error if not done in 15 seconds
			return errors.New("Timed out waiting for RecvCommit")
		}
		// break when everyone replies
		if commitTotalCount == len(pn.allNodes) {
			break
		}
	}
	// check majority
	if commitSuccessCount <= len(pn.allNodes)/2 {
		return errors.New("Couldn't achieve majority in Commit")
	}

	reply.V = vForAccept
	return nil
}

// Desc:
// GetValue looks up the value for a key, and replies with the value or with
// the Status KeyNotFound.
//
// Params:
// args: the key to check
// reply: the value and status for this lookup of the given key
func (pn *paxosNode) GetValue(args *paxosrpc.GetValueArgs, reply *paxosrpc.GetValueReply) error {
	val, found := pn.getFromStorage(args.Key)
	if found {
		reply.V = val
		reply.Status = paxosrpc.KeyFound
	} else {
		reply.Status = paxosrpc.KeyNotFound
	}
	return nil
}

// Desc:
// Receive a Prepare message from another Paxos Node. The message contains
// the key whose value is being proposed by the node sending the prepare
// message. This function should respond with Status OK if the prepare is
// accepted and Reject otherwise.
//
// Params:
// args: the Prepare Message, you must include RequesterId when you call this API
// reply: the Prepare Reply Message
func (pn *paxosNode) RecvPrepare(args *paxosrpc.PrepareArgs, reply *paxosrpc.PrepareReply) error {
	// Check highest N for that key, compare, reject if n <= N
	Np, found := pn.getFromHighestNForKey(args.Key)
	if found {
		if args.N <= Np {
			reply.Status = paxosrpc.Reject
			return nil
		}
	}
	pn.putToHighestNForKey(args.Key, args.N)

	//Check already_accepted tuples for that key, take one with highest n and add to response.
	reply.Status = paxosrpc.OK
	aaTuple, found := pn.getFromAlreadyAccepted(args.Key)
	if !found {
		reply.N_a = -1
		reply.V_a = nil
	} else {
		reply.N_a = aaTuple.propNum
		reply.V_a = aaTuple.value
	}
	return nil
}

// Desc:
// Receive an Accept message from another Paxos Node. The message contains
// the key whose value is being proposed by the node sending the accept
// message. This function should respond with Status OK if the prepare is
// accepted and Reject otherwise.
//
// Params:
// args: the Please Accept Message, you must include RequesterId when you call this API
// reply: the Accept Reply Message
func (pn *paxosNode) RecvAccept(args *paxosrpc.AcceptArgs, reply *paxosrpc.AcceptReply) error {
	// Reject if n < Np
	Np, found := pn.getFromHighestNForKey(args.Key)
	if found {
		if args.N < Np {
			reply.Status = paxosrpc.Reject
			return nil
		}
	}
	// Add (n,v) to already accepted tuples.
	newTuple := aaTuple{
		propNum: args.N,
		value:   args.V,
	}
	pn.putToAlreadyAccepted(args.Key, newTuple)

	//reply OK
	reply.Status = paxosrpc.OK

	return nil
}

// Desc:
// Receive a Commit message from another Paxos Node. The message contains
// the key whose value was proposed by the node sending the commit
// message.
//
// Params:
// args: the Commit Message, you must include RequesterId when you call this API
// reply: the Commit Reply Message
func (pn *paxosNode) RecvCommit(args *paxosrpc.CommitArgs, reply *paxosrpc.CommitReply) error {
	//store
	pn.putToStorage(args.Key, args.V)
	pn.alreadyAcceptedMux.Lock()
	delete(pn.alreadyAccepted, args.Key)
	pn.alreadyAcceptedMux.Unlock()

	return nil
}

// Desc:
// Notify another node of a replacement server which has started up. The
// message contains the Server ID of the node being replaced, and the
// hostport of the replacement node
//
// Params:
// args: the id and the hostport of the server being replaced
// reply: no use
func (pn *paxosNode) RecvReplaceServer(args *paxosrpc.ReplaceServerArgs, reply *paxosrpc.ReplaceServerReply) error {
	//update allNodes, dialing back to this new node
	newClient, err := rpc.DialHTTP("tcp", args.Hostport)

	// retries in case DialHTTP fails
	numRetries := 5
	i := 1
	for ; err != nil && i < numRetries; i++ {
		newClient, err = rpc.DialHTTP("tcp", args.Hostport)
		time.Sleep(time.Second * 1)
	}
	pn.allNodes[args.SrvID] = newClient

	return err
}

// Desc:
// Request the value that was agreed upon for a particular round. A node
// receiving this message should reply with the data (as an array of bytes)
// needed to make the replacement server aware of the keys and values
// committed so far.
//
// Params:
// args: no use
// reply: a byte array containing necessary data used by replacement server to recover
func (pn *paxosNode) RecvReplaceCatchup(args *paxosrpc.ReplaceCatchupArgs, reply *paxosrpc.ReplaceCatchupReply) error {
	// send storage; anything else?
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	encoder.Encode(pn.storage)
	reply.Data = buf.Bytes()

	return nil
}

func (pn *paxosNode) getFromHighestNForKey(key string) (int, bool) {
	pn.highestNForKeyMux.Lock()
	val, found := pn.highestNForKey[key]
	pn.highestNForKeyMux.Unlock()
	return val, found
}

func (pn *paxosNode) putToHighestNForKey(key string, val int) {
	pn.highestNForKeyMux.Lock()
	pn.highestNForKey[key] = val
	pn.highestNForKeyMux.Unlock()
}

func (pn *paxosNode) getFromStorage(key string) (interface{}, bool) {
	pn.storageMux.Lock()
	val, found := pn.storage[key]
	pn.storageMux.Unlock()
	return val, found
}

func (pn *paxosNode) putToStorage(key string, val interface{}) {
	pn.storageMux.Lock()
	pn.storage[key] = val
	pn.storageMux.Unlock()
}

func (pn *paxosNode) getFromAlreadyAccepted(key string) (aaTuple, bool) {
	pn.alreadyAcceptedMux.Lock()
	val, found := pn.alreadyAccepted[key]
	pn.alreadyAcceptedMux.Unlock()
	return val, found
}

func (pn *paxosNode) putToAlreadyAccepted(key string, val aaTuple) {
	pn.alreadyAcceptedMux.Lock()
	pn.alreadyAccepted[key] = val
	pn.alreadyAcceptedMux.Unlock()
}

func (pn *paxosNode) getFromLastPropNum(key string) (int, bool) {
	pn.lastPropNumMux.Lock()
	val, found := pn.lastPropNum[key]
	pn.lastPropNumMux.Unlock()
	return val, found
}

func (pn *paxosNode) putToLastPropNum(key string, val int) {
	pn.lastPropNumMux.Lock()
	pn.lastPropNum[key] = val
	pn.lastPropNumMux.Unlock()
}
