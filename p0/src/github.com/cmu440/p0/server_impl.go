// Implementation of a KeyValueServer. Students should write their code in this file.

package p0

import (
	"bufio"
	"bytes"
	"net"
	"strconv"
)

type keyValueServer struct {
	// TODO: implement this!
	cNumClients chan int
	cClose      chan int
}

// New creates and returns (but does not start) a new KeyValueServer.
func New() KeyValueServer {
	// TODO: implement this!
	init_db()

	return &keyValueServer{make(chan int), make(chan int)}
}

func (kvs *keyValueServer) Start(port int) error {
	// TODO: implement this!

	addr := net.JoinHostPort("", strconv.Itoa(port))

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	go runServer(ln, kvs)

	return nil
}

func (kvs *keyValueServer) Close() {
	// TODO: implement this!
	kvs.cClose <- 1
}

func (kvs *keyValueServer) Count() int {
	// TODO: implement this!
	kvs.cNumClients <- 1
	return <-kvs.cNumClients
}

// TODO: add additional methods/functions below!

func runServer(ln net.Listener, kvs *keyValueServer) {

	numClients := 0

	//slice to keep track of buffers for all clients
	gwQueues := make([]chan []byte, 0)

	//keep track of all connections
	cnList := make([]net.Conn, 0)

	//channel for listener thread to pass created client queues
	qChan := make(chan chan []byte)

	//channel for listener thread to pass created connections
	cnChan := make(chan net.Conn)

	//all inbound requests from clients (put/get)
	rQueue := make(chan [][]byte)

	//start listener thread to handle clients
	go func(ln net.Listener, qChan chan chan []byte,
		rQueue chan [][]byte, cnChan chan net.Conn) {

		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}

			cnChan <- conn
			go handleConnection(conn, rQueue, qChan)
		}
	}(ln, qChan, rQueue, cnChan)

	for {

		select {
		//server api requesting count
		case <-kvs.cNumClients:
			kvs.cNumClients <- numClients

		case newQueue := <-qChan:

			bDel := false
			delIdx := 0
			//check if we're deleting or adding
			for idx, que := range gwQueues {
				if newQueue == que {
					bDel = true
					delIdx = idx
					break
				}
			}

			if bDel == true {
				gwQueues = append(gwQueues[:delIdx], gwQueues[delIdx+1:]...)
				numClients += -1
			} else {
				gwQueues = append(gwQueues, newQueue)
				numClients += 1
			}

		case req := <-rQueue:
			parseRequest(req, gwQueues)
		case cn := <-cnChan:
			cnList = append(cnList, cn)
		case <-kvs.cClose:
			for _, cn := range cnList {
				cn.Close()
			}

			//close writing routines
			for _, que := range gwQueues {
				close(que)
			}

			//close listener
			ln.Close()

			return
		}

	}

}

func handleConnection(conn net.Conn, rQueue chan [][]byte,
	qChan chan chan []byte) {

	reader := bufio.NewReader(conn)

	gwQueue := make(chan []byte, 500)
	qChan <- gwQueue

	//start subroutine to process responses to get requests
	go func(gwQueue chan []byte, conn net.Conn) {

		for {
			select {
			case gVal, ok := <-gwQueue:
				if !ok {
					//happens when queue is closed
					return
				} else {

					_, err := conn.Write(gVal)
					conn.Write([]byte("\n"))

					if err != nil {
						return
					}

				}
			}
		}

	}(gwQueue, conn)

	for {

		byteStr, err := reader.ReadBytes('\n')
		if err != nil {

			conn.Close()

			qChan <- gwQueue

			return
		}

		//parse request, remove newline char
		splitStr := bytes.Split(byteStr[:len(byteStr)-1], []byte(","))

		rQueue <- splitStr

	}
}

func parseRequest(req [][]byte, gwQueues []chan []byte) {

	if bytes.Equal([]byte("get"), req[0]) && (len(req) == 2) {
		res := get(string(req[1]))

		resBuf := bytes.NewBuffer(req[1])
		resBuf.WriteByte(',')
		resBuf.Write(res)
		for _, ch := range gwQueues {
			if len(ch) < cap(ch) {
				ch <- resBuf.Bytes()
			}
		}
	}

	if bytes.Equal([]byte("put"), req[0]) && (len(req) == 3) {
		put(string(req[1]), req[2])
	}
}
