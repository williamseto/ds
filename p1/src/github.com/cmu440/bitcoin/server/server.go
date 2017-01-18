package main

import (
	"container/list"
	"encoding/json"
	"fmt"
	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
	"log"
	"math"
	"os"
	"strconv"
)

type server struct {
	lspServer lsp.Server
}

type RequestStatus struct {
	BestHashVal uint64
	BestNonce   uint64
	PendingReqs []int
}

type RequestPart struct {
	ReqID int
	Msg   string
	Lower uint64
	Upper uint64
}

func startServer(port int) (*server, error) {

	lspServer, err := lsp.NewServer(port, lsp.NewParams())

	theServer := &server{lspServer}

	return theServer, err
}

var LOGF *log.Logger

func main() {
	// You may need a logger for debug purpose
	const (
		name = "log.txt"
		flag = os.O_RDWR | os.O_CREATE
		perm = os.FileMode(0666)
	)

	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return
	}
	defer file.Close()

	LOGF = log.New(file, "", log.Lshortfile|log.Lmicroseconds)
	// Usage: LOGF.Println() or LOGF.Printf()

	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <port>", os.Args[0])
		return
	}

	port, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Port must be a number:", err)
		return
	}

	srv, err := startServer(port)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("Server listening on port", port)

	//defer srv.lspServer.Close()

	//read client requests

	var recMsg bitcoin.Message

	//fifo queue to distribute tasks
	availMiners := make([]int, 0)

	// maps connection ids to client type (1 is 'client', 2 is 'miner')
	clientTypeMap := make(map[int]int)

	// map each miner to the current RequestPart ID its working on
	minerReqMap := make(map[int]int)

	// unique ID to keep track of all request chunks
	reqPartID := 0

	totalMiners := uint64(0)

	reqList := list.New()

	//keep track of request status for each client
	clientReqStatMap := make(map[int]RequestStatus)

	//map request parts to client ids
	reqPartClientMap := make(map[int]int)

	//map request parts to miner ids
	reqPartMinerMap := make(map[int]int)

	for {

		connID, recBuf, err := srv.lspServer.Read()

		if err != nil {

			//client crash, delete all request parts associated with it
			if clientTypeMap[connID] == 1 {

				cliStat := clientReqStatMap[connID]
				reqSlice := cliStat.PendingReqs
				for _, id := range reqSlice {

					//delete all request parts
					delete(reqPartClientMap, id)
				}
				//finally, delete the request status for the client
				delete(clientReqStatMap, connID)
			} else {
				//should be miner case

				totalMiners = totalMiners - 1

				//remove from availMiners, if necessary
				for idx, id := range availMiners {
					if id == connID {
						//delete without preserving order
						availMiners[idx] = availMiners[len(availMiners)-1]
						availMiners[len(availMiners)-1] = 0
						availMiners = availMiners[:len(availMiners)-1]
						break
					}
				}
				delete(minerReqMap, connID)

				for partID, minerID := range reqPartMinerMap {

					//only need to delete 1?
					if minerID == connID {
						delete(reqPartMinerMap, partID)
						break
					}

				}
			}

		} else {

			err = json.Unmarshal(recBuf, &recMsg)

			switch recMsg.Type {
			case bitcoin.Request:

				clientTypeMap[connID] = 1

				//check how many miners we have available, divide up task

				n := uint64(0)

				reqSlice := make([]int, 0)

				step := uint64(1)
				if totalMiners > step {
					step = totalMiners
				}

				step = (recMsg.Upper + 1) / step

				step = 10000

				for n = 0; n+step <= recMsg.Upper; n += step {
					tmpPart := RequestPart{reqPartID, recMsg.Data,
						n, n + step - 1}
					reqList.PushBack(tmpPart)

					reqSlice = append(reqSlice, reqPartID)
					reqPartClientMap[reqPartID] = connID
					reqPartID++
				}

				if n <= recMsg.Upper {

					tmpPart := RequestPart{reqPartID, recMsg.Data, n,
						recMsg.Upper}
					reqList.PushBack(tmpPart)
					reqSlice = append(reqSlice, reqPartID)
					reqPartClientMap[reqPartID] = connID
					reqPartID++
				}

				cliReqStat := RequestStatus{math.MaxUint64, 0, reqSlice}
				clientReqStatMap[connID] = cliReqStat

			case bitcoin.Join:

				clientTypeMap[connID] = 2
				totalMiners = totalMiners + 1

				availMiners = append(availMiners, connID)

			case bitcoin.Result:

				availMiners = append(availMiners, connID)

				//remove request part from list
				reqID := minerReqMap[connID]

				for r := reqList.Front(); r != nil; r = r.Next() {

					if req, ok := r.Value.(RequestPart); ok {
						if req.ReqID == reqID {

							reqList.Remove(r)
							break
						}
					}
				}
				delete(reqPartMinerMap, reqID)

				//update request status for client
				cliID := reqPartClientMap[reqID]
				cliReqStat := clientReqStatMap[cliID]

				if recMsg.Hash < cliReqStat.BestHashVal {
					cliReqStat.BestHashVal = recMsg.Hash
					cliReqStat.BestNonce = recMsg.Nonce
				}

				reqSlice := cliReqStat.PendingReqs
				fmt.Println(reqSlice, len(reqSlice))
				for idx, id := range reqSlice {
					if id == reqID {
						//delete without preserving ordr
						reqSlice[idx] = reqSlice[len(reqSlice)-1]
						reqSlice[len(reqSlice)-1] = 0
						reqSlice = reqSlice[:len(reqSlice)-1]
						break
					}
				}
				fmt.Println(reqSlice, len(reqSlice))

				//update map
				cliReqStat.PendingReqs = reqSlice
				clientReqStatMap[cliID] = cliReqStat

				//remove entry from request part to client map
				delete(reqPartClientMap, reqID)

				//check if all request parts complete
				if len(reqSlice) == 0 {

					resMsg := bitcoin.NewResult(cliReqStat.BestHashVal,
						cliReqStat.BestNonce)
					resMsgM, _ := json.Marshal(resMsg)
					srv.lspServer.Write(cliID, resMsgM)

					//also delete entry in client request status map
					delete(clientReqStatMap, cliID)
				}

			} //end switch recMsg.Type
		}

		//now check if we have outstanding requests

		reqCount := reqList.Len()
		for len(availMiners) > 0 && reqCount > 0 {

			//check if miner already working on request
			part := reqList.Front().Value.(RequestPart)
			reqList.MoveToBack(reqList.Front())
			if _, ok := reqPartMinerMap[part.ReqID]; !ok {

				minerID := availMiners[0]

				availMiners[0] = availMiners[len(availMiners)-1]
				availMiners[len(availMiners)-1] = 0
				availMiners = availMiners[:len(availMiners)-1]

				mineMsg := bitcoin.NewRequest(part.Msg, part.Lower, part.Upper)
				mineMsgM, _ := json.Marshal(mineMsg)

				srv.lspServer.Write(minerID, mineMsgM)
				minerReqMap[minerID] = part.ReqID
				reqPartMinerMap[part.ReqID] = minerID

			}
			reqCount--

		}
	}
}
