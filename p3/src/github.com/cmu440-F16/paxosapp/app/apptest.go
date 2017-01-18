package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/cmu440-F16/paxosapp/rpc/paxosrpc"
	"net/http"
	"net/rpc"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	testRegex  = flag.String("t", "", "test to run")
	paxosPorts = flag.String("paxosports", "", "ports of all nodes")
	timeout    = 15
	at         *appTester
)

type appTester struct {
	cliMap map[int]*rpc.Client
}

type testFunc struct {
	name string
	f    func(chan bool)
}

type PostProposalRequestData struct {
	Id   string
	Name string
}

func initAppTester(allPorts string) (*appTester, error) {

	// Create RPC connection to paxos nodes.
	cliMap := make(map[int]*rpc.Client)
	portList := strings.Split(allPorts, ",")
	for i, p := range portList {
		cli, err := rpc.DialHTTP("tcp", "localhost:"+p)
		if err != nil {
			return nil, fmt.Errorf("could not connect to node %d", i)
		}
		cliMap[i] = cli
	}

	tester := new(appTester)
	tester.cliMap = cliMap

	return tester, nil
}

func (at *appTester) GetValue(key string, nodeID int) (*paxosrpc.GetValueReply, error) {
	args := &paxosrpc.GetValueArgs{Key: key}
	var reply paxosrpc.GetValueReply
	err := at.cliMap[nodeID].Call("PaxosNode.GetValue", args, &reply)
	return &reply, err
}

func checkGetValueAll(key string, value string) bool {
	for id, _ := range at.cliMap {
		r, err := at.GetValue(key, id)
		if err != nil {
			printFailErr("GetValue", err)
			return false
		}
		if r.V == nil {
			fmt.Printf("FAIL: Node %d: incorrect value from GetValue on key %s. Got nil, expected %d.\n",
				id, key, value)
			return false
		}
		if r.V != value {
			fmt.Printf("FAIL: Node %d: incorrect value from GetValue on key %s. Got %d, expected %d.\n",
				id, key, r.V, value)
			return false
		}
	}
	return true
}

func printFailErr(fname string, err error) {
	fmt.Printf("FAIL: error on %s - %s", fname, err)
}

func runTest(t testFunc, doneChan chan bool) {
	go t.f(doneChan)

	var pass bool
	select {
	case <-time.After(time.Duration(timeout) * time.Second):
		fmt.Println("FAIL: test timed out")
		pass = false
	case pass = <-doneChan:
	}

	if pass {
		fmt.Println("PASS")
	}
}

/**
 * Tests that a single proposal should be able to
 * successfully be committed to every node.
 */
func testSingleClientSingleProposal(doneChan chan bool) {
	url := "http://localhost:8080/post_proposal"

	fmt.Println("Proposing 10 values...")
	for i := 0; i < 10; i++ {
		reqData := PostProposalRequestData{
			Id:   fmt.Sprintf("test_id_%d", i),
			Name: fmt.Sprintf("test_name_%d", i),
		}
		jsonStr, err := json.Marshal(reqData)
		req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))
		req.Header.Set("Content-Type", "application/json")
		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			fmt.Println(err)
		}
		defer resp.Body.Close()
	}

	fmt.Println("Checking if each value match between servers...")
	for i := 1; i < 11; i++ {
		if !checkGetValueAll(strconv.Itoa(i), fmt.Sprintf("test_id_%d", i-1)) {
			doneChan <- false
			return
		}
	}

	doneChan <- true
}

func main() {
	btests := []testFunc{
		{"testSingleClientSingleProposal", testSingleClientSingleProposal},
	}

	flag.Parse()

	// Run the tests with a single tester
	appTester, err := initAppTester(*paxosPorts)
	if err != nil {
		fmt.Println("Failed to initialize test:", err)
	}
	at = appTester

	doneChan := make(chan bool)
	for _, t := range btests {
		if b, err := regexp.MatchString(*testRegex, t.name); b && err == nil {
			fmt.Printf("Running %s:\n", t.name)
			runTest(t, doneChan)
		}
	}
}
