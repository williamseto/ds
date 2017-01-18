// Contains the implementation of a LSP client.

package lsp

import (
	"container/list"
	"encoding/json"
	"errors"
	"github.com/cmu440/lspnet"
	"time"
)

type client struct {
	cParams    *Params
	cn         *lspnet.UDPConn
	saddr      *lspnet.UDPAddr
	connId     int
	readChan   chan Message
	writeChan  chan *Message
	ackChan    chan Message
	idChan     chan int
	sigCloseCh chan int
	closeRtCh  chan int
	timeoutCh  chan int
}

// NewClient creates, initiates, and returns a new client. This function
// should return after a connection with the server has been established
// (i.e., the client has received an Ack message from the server in response
// to its connection request), and should return a non-nil error if a
// connection could not be made (i.e., if after K epochs, the client still
// hasn't received an Ack message from the server in response to its K
// connection requests).
//
// hostport is a colon-separated string identifying the server's host address
// and port number (i.e., "localhost:9999").
func NewClient(hostport string, params *Params) (Client, error) {

	theClient := &client{params, nil, nil, 0,
		make(chan Message, 1000), make(chan *Message),
		make(chan Message), make(chan int),
		make(chan int), make(chan int, 1), make(chan int)}

	uaddr, err := lspnet.ResolveUDPAddr("udp", hostport)
	if err != nil {
		return theClient, err
	}

	conn, derr := lspnet.DialUDP("udp", nil, uaddr)
	if derr != nil {
		return theClient, derr
	}

	theClient.cn = conn
	theClient.saddr = uaddr

	//initiate connection to server
	cnMsg := NewConnect()

	//create a channel and pass to the read thread
	//the read thread will inform if ack for connect req received
	checkConnChan := make(chan bool)

	//now, spawn go routine to process events
	go ClientEventHandler(theClient, checkConnChan)

	theClient.writeChan <- cnMsg

	select {
	//the case to disconnect after K epochs should go here
	case <-time.After(time.Duration(theClient.cParams.EpochLimit*
		theClient.cParams.EpochMillis) *
		time.Millisecond):

		theClient.sigCloseCh <- 1
		return theClient, errors.New("couldnt connect after K epochs")
	case <-checkConnChan:
	}

	return theClient, nil
}

func (c *client) ConnID() int {
	c.idChan <- -1
	return <-c.idChan
}

func (c *client) Read() ([]byte, error) {

	select {
	case rMsg := <-c.readChan:
		if rMsg.SeqNum == -1 {
			return nil, errors.New("Read() timed out")
		}
		return rMsg.Payload, nil
	}
}

func (c *client) Write(payload []byte) error {

	c.timeoutCh <- 0
	select {
	case val := <-c.timeoutCh:
		if val == 1 {
			return errors.New("client write failed due to server timeout")
		}
	}

	//the write thread manages the seq numbers, it will update it
	//it will also update the connId due to race condition
	sMsg := NewData(0, 0, len(payload), payload)

	c.writeChan <- sMsg

	return nil
}

func (c *client) Close() error {

	c.sigCloseCh <- 1

	select {
	case <-c.sigCloseCh:
	}
	return nil
}

func ClientEventHandler(cli *client, cnChan chan bool) {

	rawMsgChan := make(chan Message)
	rSeqNum := 0
	pendingList := list.New()

	//seq num for outgoing messages
	seqNum := 0
	writeMsgList := list.New()
	isClosing := false
	timeoutCount := 0

	go readThread(cli, rawMsgChan)

	timer := time.NewTicker(time.Duration(cli.cParams.EpochMillis) *
		time.Millisecond)

	for {
		select {
		case <-cli.timeoutCh:
			if timeoutCount >= cli.cParams.EpochLimit {
				cli.timeoutCh <- 1
			}
			cli.timeoutCh <- -1
		case <-cli.sigCloseCh:
			isClosing = true

			//closing condition
			if isClosing && writeMsgList.Len() == 0 {
				cli.cn.Close()
				cli.closeRtCh <- 1
				cli.sigCloseCh <- 1
				return
			}

		case <-cli.idChan:
			cli.idChan <- cli.connId

		case rMsg := <-rawMsgChan:
			timeoutCount = 0

			switch rMsg.Type {

			case MsgData:

				//check if we're getting any duplicate messages
				rSeqNum = ClientProcessIncomingMsg(cli.readChan, rMsg,
					rSeqNum, pendingList)

				aMsg := NewAck(cli.connId, rMsg.SeqNum)
				aMsgM, _ := json.Marshal(aMsg)
				cli.cn.Write(aMsgM)

			case MsgAck:

				if rMsg.SeqNum == 0 && cli.connId != 0 {
					break
				}

				//check if ack for connection
				if rMsg.SeqNum == 0 {
					cnChan <- true
					cli.connId = rMsg.ConnID
				}
				for m := writeMsgList.Front(); m != nil; m = m.Next() {

					if msg, ok := m.Value.(*Message); ok {
						if msg.SeqNum == rMsg.SeqNum {

							writeMsgList.Remove(m)
							break
						}
					}

				}
				//after receiving, we can slide window and send 1 more msg
				if writeMsgList.Len() > 0 {
					sMsg := writeMsgList.Front().Value.(*Message)
					sMsgM, _ := json.Marshal(sMsg)
					cli.cn.Write(sMsgM)
				}

				//closing condition
				if isClosing && writeMsgList.Len() == 0 {
					cli.cn.Close()
					cli.closeRtCh <- 1
					cli.sigCloseCh <- 1
					return
				}
			}
		case sMsg := <-cli.writeChan:

			sMsg.SeqNum = seqNum
			sMsg.ConnID = cli.connId
			writeMsgList.PushBack(sMsg)

			if writeMsgList.Len() <= cli.cParams.WindowSize {
				sMsgM, _ := json.Marshal(sMsg)
				cli.cn.Write(sMsgM)
			}

			seqNum = seqNum + 1

		case <-timer.C:

			// Iterate through list and resend msgs.
			count := 0
			for m := writeMsgList.Front(); m != nil; m = m.Next() {

				if count >= cli.cParams.WindowSize {
					break
				}

				if msg, ok := m.Value.(*Message); ok {
					sMsgM, _ := json.Marshal(msg)
					cli.cn.Write(sMsgM)
					count++
				}
			}

			timeoutCount++

			//inform Read() calls of server timeout
			if timeoutCount >= cli.cParams.EpochLimit {
				dcMsg := Message{ConnID: cli.connId, SeqNum: -1}
				cli.readChan <- dcMsg
			}

			//requirement2: if no msgs received from server yet, send ack
			if timeoutCount < cli.cParams.EpochLimit {
				aMsg := NewAck(cli.connId, 0)
				aMsgM, _ := json.Marshal(aMsg)
				cli.cn.Write(aMsgM)
			}
			if (timeoutCount >= cli.cParams.EpochLimit) && isClosing {
				cli.cn.Close()
				cli.closeRtCh <- 1
				cli.sigCloseCh <- 1
				return
			}
		}
	}
}

func readThread(cli *client, rawMsgChan chan Message) {

	rBuf := make([]byte, 1500)

	var rMsg Message
	for {
		select {
		case <-cli.closeRtCh:
			return
		default:
			nRead, err := cli.cn.Read(rBuf)

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

			//pass to eventhandler for further processing
			rawMsgChan <- rMsg
		}

	}
}

func ClientProcessIncomingMsg(readChan chan Message, rMsg Message,
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

		//default (empty list; rMsg has greatest seq num)
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
