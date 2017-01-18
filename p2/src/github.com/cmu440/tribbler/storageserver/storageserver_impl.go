package storageserver

import (
	"fmt"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

type LeaseInfo struct {
	Port string
	Time time.Time
}

type storageServer struct {
	nodeList      []storagerpc.Node
	myNodeID      uint32
	numNodes      int
	storageMap    map[string]interface{}
	leaseMap      map[string][]LeaseInfo
	updateMap     map[string]bool
	storageMapMux sync.Mutex
	leaseMapMux   sync.Mutex
	updateMapMux  sync.Mutex
	cliConnMap    map[string]*rpc.Client
	connMapMux    sync.Mutex
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
	myPort := fmt.Sprintf(":%d", port)

	storageServer := new(storageServer)
	storageServer.nodeList = make([]storagerpc.Node, 1)
	storageServer.myNodeID = nodeID
	storageServer.nodeList[0] = storagerpc.Node{myPort, nodeID}
	storageServer.numNodes = numNodes
	storageServer.storageMap = make(map[string]interface{})
	storageServer.leaseMap = make(map[string][]LeaseInfo)
	storageServer.updateMap = make(map[string]bool)
	storageServer.cliConnMap = make(map[string]*rpc.Client)

	// Create the server socket that will listen for incoming RPCs.
	listener, err := net.Listen("tcp", myPort)
	if err != nil {
		return nil, err
	}

	// Wrap the storageServer before registering it for RPC.
	err = rpc.RegisterName("StorageServer", storagerpc.Wrap(storageServer))
	if err != nil {
		return nil, err
	}

	// Setup the HTTP handler that will server incoming RPCs and
	// serve requests in a background goroutine.
	rpc.HandleHTTP()
	go http.Serve(listener, nil)

	// if slave, then register with master
	if masterServerHostPort != "" {

		cli, err := rpc.DialHTTP("tcp", masterServerHostPort)
		if err != nil {
			return nil, err
		}
		args := &storagerpc.RegisterArgs{storageServer.nodeList[0]}
		var reply storagerpc.RegisterReply
		cli.Call("StorageServer.RegisterServer", args, &reply)

		for reply.Status != storagerpc.OK {
			time.Sleep(time.Second)
			cli.Call("StorageServer.RegisterServer", args, &reply)
		}
	} else {
		// master just waits
		for len(storageServer.nodeList) < numNodes {
			time.Sleep(100 * time.Millisecond)
		}
	}

	return storageServer, nil
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	ss.nodeList = append(ss.nodeList, args.ServerInfo)
	reply.Servers = ss.nodeList
	if len(ss.nodeList) < ss.numNodes {
		reply.Status = storagerpc.NotReady
	} else {
		reply.Status = storagerpc.OK
	}
	return nil
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	reply.Servers = ss.nodeList
	if len(ss.nodeList) < ss.numNodes {
		reply.Status = storagerpc.NotReady
	} else {
		reply.Status = storagerpc.OK
	}
	return nil
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	// If the key does not fall within the storage server's range, it should reply with status WrongServer.
	if !ss.IsKeyInRange(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	val, found := ss.getFromStorageMap(args.Key)
	if found {
		reply.Status = storagerpc.OK
		reply.Value = val.(string)
	} else {
		reply.Status = storagerpc.KeyNotFound
	}

	// grant lease
	if args.WantLease {

		// don't grant if key is being updated
		if updating, _ := ss.getFromUpdateMap(args.Key); updating {
			return nil
		}

		reply.Lease.Granted = true
		reply.Lease.ValidSeconds = storagerpc.LeaseSeconds
		lease := LeaseInfo{args.HostPort, time.Now()}

		if list, ok := ss.getFromLeaseMap(args.Key); ok {
			// append to list of leases
			list = append(list, lease)
			ss.putToLeaseMap(args.Key, list)
		} else {
			// create new list for this key
			list := make([]LeaseInfo, 1)
			list[0] = lease
			ss.putToLeaseMap(args.Key, list)
		}
	}
	return nil
}

func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	// If the key does not fall within the storage server's range, it should reply with status WrongServer.
	if !ss.IsKeyInRange(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	_, found := ss.getFromStorageMap(args.Key)
	if !found {
		reply.Status = storagerpc.KeyNotFound
	} else {
		ss.storageMapMux.Lock()
		delete(ss.storageMap, args.Key)
		ss.storageMapMux.Unlock()
		reply.Status = storagerpc.OK
	}
	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	// If the key does not fall within the storage server's range, it should reply with status WrongServer.
	if !ss.IsKeyInRange(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	val, found := ss.getFromStorageMap(args.Key)
	if found {
		reply.Status = storagerpc.OK
		// need to allocate since copy takes min of len(src), len(dst)
		reply.Value = make([]string, len(val.([]string)))
		copy(reply.Value, val.([]string))
	} else {
		reply.Status = storagerpc.KeyNotFound
	}

	// grant lease
	if args.WantLease {

		// don't grant if key is being updated
		if updating, _ := ss.getFromUpdateMap(args.Key); updating {
			return nil
		}

		reply.Lease.Granted = true
		reply.Lease.ValidSeconds = storagerpc.LeaseSeconds
		lease := LeaseInfo{args.HostPort, time.Now()}

		if list, ok := ss.getFromLeaseMap(args.Key); ok {
			// append to list of leases
			list = append(list, lease)
			ss.putToLeaseMap(args.Key, list)
		} else {
			// create new list for this key
			list := make([]LeaseInfo, 1)
			list[0] = lease
			ss.putToLeaseMap(args.Key, list)
		}
	}
	return nil
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	// If the key does not fall within the storage server's range, it should reply with status WrongServer.
	if !ss.IsKeyInRange(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	// before updating, stop granting leases and revoke existing
	ss.putToUpdateMap(args.Key, true)

	ss.RevokeKeyLeases(args.Key)

	ss.putToStorageMap(args.Key, args.Value)
	reply.Status = storagerpc.OK

	// allow lease granting again
	ss.putToUpdateMap(args.Key, false)

	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	// If the key does not fall within the storage server's range, it should reply with status WrongServer.
	if !ss.IsKeyInRange(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	key := args.Key

	// before updating, stop granting leases and revoke existing
	ss.putToUpdateMap(args.Key, true)

	ss.RevokeKeyLeases(args.Key)

	val, found := ss.getFromStorageMap(key)
	var oldList []string
	// if list exists, make sure value isn't already contained
	if found {
		oldList = val.([]string)
		for _, elem := range oldList {
			if elem == args.Value {
				reply.Status = storagerpc.ItemExists
				return nil
			}
		}
	} else {
		oldList = make([]string, 0)
	}
	// append and put
	ss.putToStorageMap(key, append(oldList, args.Value))
	reply.Status = storagerpc.OK

	// allow lease granting again
	ss.putToUpdateMap(args.Key, false)

	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	// If the key does not fall within the storage server's range, it should reply with status WrongServer.
	if !ss.IsKeyInRange(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	key := args.Key

	val, found := ss.getFromStorageMap(key)
	// to succeed, there must be a list with the matching value
	if found {
		list := val.([]string)
		for i, elem := range list {
			if elem == args.Value {
				// replace the list with one without the elem at index i
				ss.putToStorageMap(key, append(list[:i], list[i+1:]...))
				reply.Status = storagerpc.OK
				return nil
			}
		}
	}
	reply.Status = storagerpc.ItemNotFound
	return nil
}

func (ss *storageServer) IsKeyInRange(key string) bool {
	hash := libstore.StoreHash(key)
	// find closest bucket
	var minDistance uint32 = math.MaxUint32
	var closestBucketID uint32
	for _, node := range ss.nodeList {
		var dist uint32
		nodeID := node.NodeID
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

	if closestBucketID != ss.myNodeID {
		return false
	}
	return true
}

func (ss *storageServer) RevokeKeyLeases(key string) {
	if list, ok := ss.getFromLeaseMap(key); ok {
		args := &storagerpc.RevokeLeaseArgs{key}
		var reply storagerpc.RevokeLeaseReply

		for _, lease := range list {
			duration := time.Since(lease.Time)
			leaseLimit := float64(storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds)
			if duration.Seconds() < leaseLimit {

				var cli *rpc.Client
				if cli, ok = ss.cliConnMap[lease.Port]; !ok {
					rpcClient, err := rpc.DialHTTP("tcp", lease.Port)
					if err == nil {
						ss.putToConnMap(lease.Port, rpcClient)
						cli = rpcClient
					}
				}
				if cli != nil {
					call := cli.Go("LeaseCallbacks.RevokeLease", args, &reply, nil)

					timer := time.NewTimer(time.Second * time.Duration(leaseLimit-duration.Seconds()))
					select {
					case <-call.Done:
					case <-timer.C:
						// consider done if lease expires
					}
				}
			}
		}
		// clear lease list
		ss.leaseMapMux.Lock()
		delete(ss.leaseMap, key)
		ss.leaseMapMux.Unlock()
	}
}

func (ss *storageServer) getFromStorageMap(key string) (interface{}, bool) {
	ss.storageMapMux.Lock()
	val, found := ss.storageMap[key]
	ss.storageMapMux.Unlock()
	return val, found
}

func (ss *storageServer) putToStorageMap(key string, val interface{}) {
	ss.storageMapMux.Lock()
	ss.storageMap[key] = val
	ss.storageMapMux.Unlock()
}

func (ss *storageServer) getFromLeaseMap(key string) ([]LeaseInfo, bool) {
	ss.leaseMapMux.Lock()
	val, found := ss.leaseMap[key]
	ss.leaseMapMux.Unlock()
	return val, found
}

func (ss *storageServer) putToLeaseMap(key string, val []LeaseInfo) {
	ss.leaseMapMux.Lock()
	ss.leaseMap[key] = val
	ss.leaseMapMux.Unlock()
}

func (ss *storageServer) getFromUpdateMap(key string) (bool, bool) {
	ss.updateMapMux.Lock()
	val, found := ss.updateMap[key]
	ss.updateMapMux.Unlock()
	return val, found
}

func (ss *storageServer) putToUpdateMap(key string, val bool) {
	ss.updateMapMux.Lock()
	ss.updateMap[key] = val
	ss.updateMapMux.Unlock()
}

func (ss *storageServer) putToConnMap(key string, val *rpc.Client) {
	ss.connMapMux.Lock()
	ss.cliConnMap[key] = val
	ss.connMapMux.Unlock()
}
