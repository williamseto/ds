package main

import (
	"encoding/json"
	"fmt"
	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
	"os"
)

// Attempt to connect miner as a client to the server.
func joinWithServer(hostport string) (lsp.Client, error) {

	client, err := lsp.NewClient(hostport, lsp.NewParams())
	if err != nil {
		return nil, err
	}

	joinMsg := bitcoin.NewJoin()
	joinMsgM, err := json.Marshal(joinMsg)

	err = client.Write(joinMsgM)

	if err != nil {
		return nil, err
	}

	return client, nil
}

func main() {
	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <hostport>", os.Args[0])
		return
	}

	hostport := os.Args[1]
	miner, err := joinWithServer(hostport)
	if err != nil {
		fmt.Println("Failed to join with server:", err)
		return
	}

	defer miner.Close()

	//read request messages from server
	var recMsg bitcoin.Message
	for {

		recBuf, err := miner.Read()

		if err != nil {
			break
		}

		err = json.Unmarshal(recBuf, &recMsg)

		switch recMsg.Type {
		case bitcoin.Request:

			hashMsg := recMsg.Data
			bestHashVal := uint64(18446744073709551615)
			bestNonce := uint64(0)

			for n := recMsg.Lower; n <= recMsg.Upper; n++ {
				hashVal := bitcoin.Hash(hashMsg, n)
				if hashVal < bestHashVal {
					bestHashVal = hashVal
					bestNonce = n
				}
			}

			//now write result to server
			resMsg := bitcoin.NewResult(bestHashVal, bestNonce)

			resMsgM, _ := json.Marshal(resMsg)

			miner.Write(resMsgM)

		}
	}

}
