// Contains the implementation of a LSP server.

package lsp

import (
	"container/list"
	"encoding/json"
	"errors"
	"github.com/cmu440/lspnet"
	"strconv"
	"time"
)

type server struct {
	sParams     *Params
	readChan    chan Message
	clientID    int
	connMap     map[int]*lspnet.UDPAddr
	cn          *lspnet.UDPConn
	writeChan   chan *Message
	ackChan     chan Message
	closeCnChan chan int
	readReqChan chan int
	sigCloseCh  chan int //used when Close() is called
	closeRtCh   chan int //signal read thread to return
	cliStatCh   chan int
}

type ExtendedMsg struct {
	Msg      Message
	Addr     *lspnet.UDPAddr
	IsClosed int
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {

	connMap := make(map[int]*lspnet.UDPAddr)
	theServer := &server{params, make(chan Message, 3000), 1, connMap, nil,
		make(chan *Message), make(chan Message),
		make(chan int), make(chan int),
		make(chan int), make(chan int, 1), make(chan int)}

	addr := lspnet.JoinHostPort("", strconv.Itoa(port))
	uaddr, err := lspnet.ResolveUDPAddr("udp", addr)
	if err != nil {
		return theServer, err
	}

	conn, err := lspnet.ListenUDP("udp", uaddr)
	if err != nil {
		return theServer, err
	}
	theServer.cn = conn

	go ServerEventHandler(theServer)

	return theServer, nil
}

func (s *server) Read() (int, []byte, error) {

	select {
	case rMsg := <-s.readChan:
		if rMsg.SeqNum == -1 {
			return rMsg.ConnID, nil, errors.New("client closed")
		}
		return rMsg.ConnID, rMsg.Payload, nil

	}
}

func (s *server) Write(connID int, payload []byte) error {

	//the write thread manages the seq numbers, it will update it
	sMsg := NewData(connID, 0, len(payload), payload)

	s.writeChan <- sMsg

	select {
	case val := <-s.cliStatCh:
		if val < 1 {
			return errors.New("server failed to write to client")
		}
	}

	return nil
}

func (s *server) CloseConn(connID int) error {

	s.closeCnChan <- connID

	select {
	case val := <-s.closeCnChan:
		if val < 0 {
			return errors.New("no such connID")
		} else {
			return nil
		}
	}
}

func (s *server) Close() error {

	s.sigCloseCh <- 1

	select {
	case val := <-s.sigCloseCh:
		if val == 1 {
			return nil
		}
		return errors.New("server forcefully closed due to client timeout")
	}
}

func IsFinishedWrites(msgMap map[int]*list.List) bool {
	for _, msgList := range msgMap {

		if msgList.Len() > 0 {
			return false
		}
	}
	return true
}

func ServerEventHandler(srv *server) {

	closeCnMap := make(map[int]int)

	// manage seq numbers for each client with a map
	seqNumMap := make(map[int]int)

	// hold pending (out of order) read messages in a map of lists
	pendingMap := make(map[int]*list.List)

	//maintain sequence for each client,
	//since on the client side we need to know the exact order
	cliSeqNumMap := make(map[int]int)

	// manage write message lists for each client with a map
	writeMsgMap := make(map[int]*list.List)

	rtChan := make(chan ExtendedMsg)

	isClosing := false

	go ReadThreadServer(srv, rtChan)

	timer := time.NewTicker(time.Duration(srv.sParams.EpochMillis) *
		time.Millisecond)

	for {
		select {

		case <-srv.sigCloseCh:
			isClosing = true

			//closing condition
			if isClosing && IsFinishedWrites(writeMsgMap) {
				srv.cn.Close()
				srv.closeRtCh <- 1
				srv.sigCloseCh <- 1
				return
			}

		case closeID := <-srv.closeCnChan:
			if _, ok := seqNumMap[closeID]; !ok {
				srv.closeCnChan <- -1
			} else {
				closeCnMap[closeID] = srv.sParams.EpochLimit + 1
				srv.closeCnChan <- 1
			}

		case sMsg := <-srv.writeChan:

			// do not write messages to a disconnected client
			if closeCnMap[sMsg.ConnID] >= srv.sParams.EpochLimit {
				srv.cliStatCh <- -1
			} else {
				srv.cliStatCh <- 1
			}

			//create a new list if first message to a client
			if _, ok := writeMsgMap[sMsg.ConnID]; !ok {
				newMsgList := list.New()
				writeMsgMap[sMsg.ConnID] = newMsgList
				cliSeqNumMap[sMsg.ConnID] = 1
			}

			msgList := writeMsgMap[sMsg.ConnID]

			sMsg.SeqNum = cliSeqNumMap[sMsg.ConnID]
			msgList.PushBack(sMsg)

			cliSeqNumMap[sMsg.ConnID] = sMsg.SeqNum + 1

			if msgList.Len() <= srv.sParams.WindowSize {
				sMsgM, _ := json.Marshal(sMsg)
				srv.cn.WriteToUDP(sMsgM, srv.connMap[sMsg.ConnID])
			}

		case rAMsg := <-rtChan:
			rMsg := rAMsg.Msg

			//reset client epoch timeout
			if closeCnMap[rMsg.ConnID] < srv.sParams.EpochLimit {
				closeCnMap[rMsg.ConnID] = 0
			}

			switch rMsg.Type {
			case MsgConnect:

				//in case we get duplicate connection requests
				isNewClient := true
				clientID := 0
				for id, caddr := range srv.connMap {
					if caddr.String() == rAMsg.Addr.String() {
						isNewClient = false
						clientID = id
						break
					}
				}

				if isNewClient {
					//initialize seq num entry in map
					clientID = srv.clientID
					seqNumMap[srv.clientID] = 0
					pendingMap[srv.clientID] = list.New()
					srv.connMap[srv.clientID] = rAMsg.Addr
					srv.clientID++
				}

				aMsg := NewAck(clientID, 0)
				aMsgM, _ := json.Marshal(aMsg)
				srv.cn.WriteToUDP(aMsgM, rAMsg.Addr)

			case MsgData:

				rSeqNum, _ := seqNumMap[rMsg.ConnID]
				pendingList := pendingMap[rMsg.ConnID]

				seqNumMap[rMsg.ConnID] = SrvProcessMsg(srv.readChan,
					rMsg, rSeqNum, pendingList)

				aMsg := NewAck(rMsg.ConnID, rMsg.SeqNum)
				aMsgM, _ := json.Marshal(aMsg)
				srv.cn.WriteToUDP(aMsgM, rAMsg.Addr)
			case MsgAck:

				if rMsg.SeqNum != 0 {
					msgList := writeMsgMap[rMsg.ConnID]

					if _, ok := writeMsgMap[rMsg.ConnID]; ok {
						for m := msgList.Front(); m != nil; m = m.Next() {

							if msg, ok := m.Value.(*Message); ok {
								if msg.SeqNum == rMsg.SeqNum {

									msgList.Remove(m)
									break
								}
							}
						}

						//after receiving, we can slide window, send 1 more msg
						if msgList.Len() > 0 {
							sMsg := msgList.Front().Value.(*Message)
							sMsgM, _ := json.Marshal(sMsg)
							srv.cn.WriteToUDP(sMsgM, srv.connMap[sMsg.ConnID])
						}
					}
				}

				//if server is closing, terminate after completing all msgs
				if isClosing && IsFinishedWrites(writeMsgMap) {
					srv.cn.Close()
					srv.closeRtCh <- 1
					srv.sigCloseCh <- 1
					return
				}
			} //end switch rMsg.Type

		case <-timer.C:

			// Iterate through every list and resend msgs.
			for _, msgList := range writeMsgMap {

				count := 0
				for m := msgList.Front(); m != nil; m = m.Next() {

					if count >= srv.sParams.WindowSize {
						break
					}
					if msg, ok := m.Value.(*Message); ok {
						sMsgM, _ := json.Marshal(msg)
						srv.cn.WriteToUDP(sMsgM, srv.connMap[msg.ConnID])
						count++
					}
				}
			}

			//increment client epoch timeout
			for connID, count := range closeCnMap {

				//map has a default key of 0, so skip that
				if connID > 0 {
					closeCnMap[connID] = count + 1

					//send a read error msg informing client timeout
					if count+1 == srv.sParams.EpochLimit {

						dcMsg := Message{ConnID: connID, SeqNum: -1}
						srv.readChan <- dcMsg
					}

					if closeCnMap[connID] < srv.sParams.EpochLimit {
						aMsg := NewAck(connID, 0)
						aMsgM, _ := json.Marshal(aMsg)
						srv.cn.WriteToUDP(aMsgM, srv.connMap[connID])
					}

					// server is closing and client times out, return error
					if (count+1 >= srv.sParams.EpochLimit+1) && isClosing {
						srv.cn.Close()
						srv.closeRtCh <- 1
						srv.sigCloseCh <- -1
						return
					}
				}
			}

		}
	}

}

func ReadThreadServer(srv *server, rawMsgChan chan ExtendedMsg) {

	rBuf := make([]byte, 1500)
	var rMsg Message

	for {
		select {
		case <-srv.closeRtCh:
			return

		default:

			nRead, caddr, err := srv.cn.ReadFromUDP(rBuf)

			err = json.Unmarshal(rBuf[0:nRead], &rMsg)

			if err != nil {
				continue
			}

			//now enforce payload size
			if len(rMsg.Payload) < rMsg.Size {
				continue
			}
			if len(rMsg.Payload) > rMsg.Size {
				rMsg.Payload = rMsg.Payload[:rMsg.Size]
			}

			newExtendedMsg := ExtendedMsg{Msg: rMsg, Addr: caddr}

			rawMsgChan <- newExtendedMsg
		}

	}
}

func SrvProcessMsg(readChan chan Message, rMsg Message,
	rSeqNum int, msgList *list.List) int {

	if rMsg.SeqNum != (rSeqNum + 1) {

		//if not next msg, store msg in sorted order
		for m := msgList.Front(); m != nil; m = m.Next() {

			if msg, ok := m.Value.(Message); ok {

				if rMsg.SeqNum < msg.SeqNum {

					msgList.InsertBefore(rMsg, m)
					return rSeqNum
				}
			}

		}

		//default (empty list or rMsg has greatest seq num)
		msgList.PushBack(rMsg)
		return rSeqNum
	} else {

		//should be next msg in sequence
		readChan <- rMsg
		rSeqNum++

		//now check if we can continue to remove msgs from pending list
		var next *list.Element
		for m := msgList.Front(); m != nil; m = next {

			if msg, ok := m.Value.(Message); ok {

				//need to keep separate pointer for deletion
				next = m.Next()
				if msg.SeqNum == (rSeqNum + 1) {

					msgList.Remove(m)
					readChan <- msg
					rSeqNum++
				}
			}

		}
	}

	return rSeqNum
}
