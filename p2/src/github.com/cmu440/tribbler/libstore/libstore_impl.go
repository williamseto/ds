package libstore

import (
	"errors"
	"github.com/cmu440/tribbler/rpc/librpc"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"math"
	"net/rpc"
	"sync"
	"time"
)

type libstore struct {
	myPort               string
	nodeIDToServer       map[uint32]*Server
	popularityTracker    map[string][]time.Time
	cache                map[string]*CachedValue
	leaseMode            LeaseMode
	cacheMutex           sync.Mutex
	popularityTrackerMux sync.Mutex
}

type Server struct {
	hostPort  string
	rpcClient *rpc.Client
}

type CachedValue struct {
	val          interface{}
	leaseStart   time.Time
	leaseSeconds int
}

const TRIBBLES_LIMIT = 100
const KEYNOTFOUND = "KeyNotFound"
const ITEMNOTFOUND = "ItemNotFound"

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
	cli, err := rpc.DialHTTP("tcp", masterServerHostPort)
	if err != nil {
		return nil, err
	}

	ls := &libstore{
		myPort:            myHostPort,
		nodeIDToServer:    make(map[uint32]*Server),
		popularityTracker: make(map[string][]time.Time),
		cache:             make(map[string]*CachedValue),
		leaseMode:         mode,
	}

	// wait till all storageservers are ready
	args := &storagerpc.GetServersArgs{}
	var reply storagerpc.GetServersReply

	cli.Call("StorageServer.GetServers", args, &reply)

	retryCount := 0
	for reply.Status != storagerpc.OK {

		if retryCount >= 5 {
			return nil, errors.New("failed to connect to StorageServer")
		}
		time.Sleep(time.Second)
		cli.Call("StorageServer.GetServers", args, &reply)
		retryCount++
	}
	nodes := reply.Servers
	// make the ring
	for _, node := range nodes {
		ls.nodeIDToServer[node.NodeID] = &Server{hostPort: node.HostPort}
	}

	rpc.RegisterName("LeaseCallbacks", librpc.Wrap(ls))

	// separate thread to periodically clear cache
	go func(ls *libstore) {
		for {
			time.Sleep(time.Second * 10)

			ls.cacheMutex.Lock()
			for key, cachedValue := range ls.cache {
				if time.Since(cachedValue.leaseStart) > (time.Second * time.Duration(cachedValue.leaseSeconds)) {
					delete(ls.cache, key)
				}
			}
			ls.cacheMutex.Unlock()
		}
	}(ls)

	return ls, nil
}

func (ls *libstore) Get(key string) (string, error) {
	// check cache
	ls.cacheMutex.Lock()
	cachedValue, found := ls.cache[key]
	ls.cacheMutex.Unlock()
	if found {
		// make sure lease hasn't expired
		if time.Since(cachedValue.leaseStart) < (time.Second * time.Duration(cachedValue.leaseSeconds)) {
			return cachedValue.val.(string), nil
		}
	}

	wantLease := false

	// update popularity tracker
	times, found := ls.getFromPopularityTracker(key)
	var oldTimes []time.Time
	if found {
		// cut off old ones
		for len(times) > 0 && time.Since(times[0]) > (time.Duration(storagerpc.QueryCacheSeconds)*time.Second) {
			times = times[1:]
		}
		oldTimes = times
		// check if popular
		if len(times) >= storagerpc.QueryCacheThresh {
			// check if myPort is valid (not empty)
			if ls.myPort != "" {
				wantLease = true
			}
		}
	} else {
		oldTimes = make([]time.Time, 0)
	}

	// add this query
	ls.putToPopularityTracker(key, append(oldTimes, time.Now()))

	// override lease mode
	if ls.leaseMode == Always {
		wantLease = true
	}

	// call the storageserver
	args := &storagerpc.GetArgs{Key: key, HostPort: ls.myPort, WantLease: wantLease}
	var reply storagerpc.GetReply
	rpcClient, err := ls.GetServerForKey(key)
	if err != nil {
		return "", err
	}
	err = rpcClient.Call("StorageServer.Get", args, &reply)
	if err != nil {
		return "", errors.New("StorageServer Get() RPC error: " + err.Error())
	}

	//only use rpc reply status for error checking
	if reply.Status != storagerpc.OK {
		return "", errors.New("StorageServer Get() error")
	}
	//add to cache if lease granted
	lease := reply.Lease
	if lease.Granted {
		ls.cacheMutex.Lock()
		ls.cache[key] = &CachedValue{
			val:          reply.Value,
			leaseStart:   time.Now(),
			leaseSeconds: lease.ValidSeconds,
		}
		ls.cacheMutex.Unlock()
	}

	return reply.Value, nil
}

func (ls *libstore) Put(key, value string) error {
	args := &storagerpc.PutArgs{Key: key, Value: value}
	var reply storagerpc.PutReply

	rpcClient, err := ls.GetServerForKey(key)
	if err != nil {
		return err
	}
	err = rpcClient.Call("StorageServer.Put", args, &reply)
	if err != nil {
		return errors.New("StorageServer Put() RPC error: " + err.Error())
	}

	if reply.Status == storagerpc.WrongServer {
		return errors.New("WrongServer")
	}

	return nil
}

func (ls *libstore) Delete(key string) error {
	args := &storagerpc.DeleteArgs{Key: key}
	var reply storagerpc.DeleteReply

	rpcClient, err := ls.GetServerForKey(key)
	if err != nil {
		return err
	}
	err = rpcClient.Call("StorageServer.Delete", args, &reply)
	if err != nil {
		return errors.New("StorageServer Delete() RPC error: " + err.Error())
	}

	switch reply.Status {
	case storagerpc.WrongServer:
		return errors.New("WrongServer")
	case storagerpc.KeyNotFound:
		return errors.New("KeyNotFound")
	}

	return nil
}

func (ls *libstore) GetList(key string) ([]string, error) {
	// check cache
	ls.cacheMutex.Lock()
	cachedValue, found := ls.cache[key]
	ls.cacheMutex.Unlock()
	if found {
		// make sure lease hasn't expired
		if time.Since(cachedValue.leaseStart) < (time.Second * time.Duration(cachedValue.leaseSeconds)) {
			return cachedValue.val.([]string), nil
		}
	}

	wantLease := false

	// update popularity tracker
	times, found := ls.getFromPopularityTracker(key)
	var oldTimes []time.Time
	if found {
		// cut off old ones
		for len(times) > 0 && time.Since(times[0]) > (time.Duration(storagerpc.QueryCacheSeconds)*time.Second) {
			times = times[1:]
		}
		oldTimes = times
		// check if popular
		if len(times) >= storagerpc.QueryCacheThresh {
			// check if myPort is valid (not empty)
			if ls.myPort != "" {
				wantLease = true
			}
		}
	} else {
		oldTimes = make([]time.Time, 0)
	}

	// add this query
	ls.putToPopularityTracker(key, append(oldTimes, time.Now()))

	args := &storagerpc.GetArgs{Key: key, HostPort: ls.myPort, WantLease: wantLease}
	var reply storagerpc.GetListReply

	rpcClient, err := ls.GetServerForKey(key)
	if err != nil {
		return nil, err
	}
	err = rpcClient.Call("StorageServer.GetList", args, &reply)
	if err != nil {
		return reply.Value, errors.New("StorageServer GetList() RPC error: " + err.Error())
	}

	switch reply.Status {
	case storagerpc.WrongServer:
		return reply.Value, errors.New("WrongServer")
	// this is because libtest is returning ItemNotFound
	// however, this differs from the API stating that
	// only WrongServer and KeyNotFound should be returned
	case storagerpc.ItemNotFound:
		fallthrough
	case storagerpc.KeyNotFound:
		return reply.Value, errors.New("KeyNotFound")
	}

	//add to cache if lease granted
	lease := reply.Lease
	if lease.Granted {
		ls.cacheMutex.Lock()
		ls.cache[key] = &CachedValue{
			val:          reply.Value,
			leaseStart:   time.Now(),
			leaseSeconds: lease.ValidSeconds,
		}
		ls.cacheMutex.Unlock()
	}

	return reply.Value, nil
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	args := &storagerpc.PutArgs{Key: key, Value: removeItem}
	var reply storagerpc.PutReply

	rpcClient, err := ls.GetServerForKey(key)
	if err != nil {
		return err
	}
	err = rpcClient.Call("StorageServer.RemoveFromList", args, &reply)
	if err != nil {
		return errors.New("StorageServer RemoveFromList() RPC error: " + err.Error())
	}

	switch reply.Status {
	case storagerpc.ItemNotFound:
		return errors.New("ItemNotFound")
	case storagerpc.KeyNotFound:
		return errors.New("KeyNotFound")
	}

	return nil
}

func (ls *libstore) AppendToList(key, newItem string) error {
	args := &storagerpc.PutArgs{Key: key, Value: newItem}
	var reply storagerpc.PutReply

	rpcClient, err := ls.GetServerForKey(key)
	if err != nil {
		return err
	}
	err = rpcClient.Call("StorageServer.AppendToList", args, &reply)
	if err != nil {
		return errors.New("StorageServer AppendToList() RPC error: " + err.Error())
	}

	if reply.Status == storagerpc.ItemExists {
		return errors.New("ItemExists")
	}

	return nil
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	ls.cacheMutex.Lock()
	_, found := ls.cache[args.Key]
	ls.cacheMutex.Unlock()
	// If the key did not exist in the cache, reply with status KeyNotFound
	if !found {
		reply.Status = storagerpc.KeyNotFound
	} else {
		// If the key was successfully revoked, reply with status OK
		ls.cacheMutex.Lock()
		delete(ls.cache, args.Key)
		ls.cacheMutex.Unlock()
		reply.Status = storagerpc.OK
	}
	return nil
}

func (ls *libstore) GetServerForKey(key string) (*rpc.Client, error) {
	hash := StoreHash(key)
	// find closest bucket
	var minDistance uint32 = math.MaxUint32
	var closestBucketID uint32
	for nodeID, _ := range ls.nodeIDToServer {
		var dist uint32
		if nodeID > hash {
			dist = nodeID - hash
		} else if nodeID < hash {
			dist = (math.MaxUint32 - hash) + nodeID
		} else {
			closestBucketID = nodeID
			break
		}
		if dist < minDistance {
			minDistance = dist
			closestBucketID = nodeID
		}
	}
	server, _ := ls.nodeIDToServer[closestBucketID]
	// if first time, establish and save connection
	if server.rpcClient == nil {
		// establish and save the connection
		rpcClient, err := rpc.DialHTTP("tcp", server.hostPort)
		if err != nil {
			return nil, err
		}
		server.rpcClient = rpcClient
	}
	return server.rpcClient, nil
}

func (ls *libstore) getFromPopularityTracker(key string) ([]time.Time, bool) {
	ls.popularityTrackerMux.Lock()
	val, found := ls.popularityTracker[key]
	ls.popularityTrackerMux.Unlock()
	return val, found
}

func (ls *libstore) putToPopularityTracker(key string, val []time.Time) {
	ls.popularityTrackerMux.Lock()
	ls.popularityTracker[key] = val
	ls.popularityTrackerMux.Unlock()
}
