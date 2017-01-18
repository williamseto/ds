package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/cmu440-F16/paxosapp/rpc/paxosrpc"
	"gorilla/websocket"
	"html/template"
	"net/http"
	"net/rpc"
	"strconv"
	"strings"
	"time"
)

var (
	paxosPorts = flag.String("paxosports", "", "ports of all nodes")
)

type webapp struct {
	playlist          []string
	playlistNames     map[string]string
	paxosMap          map[int]*rpc.Client
	playerConn        *websocket.Conn
	connectedToPlayer bool
}

func (app *webapp) indexHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "play or propose next\n")
}

type ProposeTemplateData struct {
	Playlist []string
}

func (app *webapp) proposeHandler(w http.ResponseWriter, r *http.Request) {
	tmpl, _ := template.ParseFiles("propose.html")

	videoNames := make([]string, len(app.playlist))
	for i, videoID := range app.playlist {
		videoNames[i] = app.playlistNames[videoID]
	}
	tmpl.Execute(w, ProposeTemplateData{videoNames})
}

type PlayerTemplateData struct {
	ID string
}

func (app *webapp) playerHandler(w http.ResponseWriter, r *http.Request) {
	tmpl, _ := template.ParseFiles("player.html")
	tmpl.Execute(w, "blank")
}

type PostProposalRequestData struct {
	Id   string
	Name string
}

// this is called when a user proposes the next video id
func (app *webapp) postProposalHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	var data PostProposalRequestData
	decoder.Decode(&data)
	defer r.Body.Close()
	videoID := data.Id

	// check with playlist for which id to propose
	nextID := strconv.Itoa(len(app.playlist) + 1)

	// loop through nodes in case some are down?
	pnArgs := &paxosrpc.ProposalNumberArgs{Key: nextID}
	var pnReply paxosrpc.ProposalNumberReply
	var pID int
	for nodeID, node := range app.paxosMap {
		err := node.Call("PaxosNode.GetNextProposalNumber", pnArgs, &pnReply)
		if err == nil {
			pID = nodeID
			break
		} else {
			fmt.Println("nodeID =", nodeID, "is dead")
		}
	}

	// paxos proposal
	pArgs := &paxosrpc.ProposeArgs{N: pnReply.N, Key: nextID, V: videoID}
	var pReply paxosrpc.ProposeReply
	err := app.paxosMap[pID].Call("PaxosNode.Propose", pArgs, &pReply)
	if err != nil {
		fmt.Println("Propose reply:", err)
	}

	result := make(map[string]interface{})
	if (err == nil) && (pReply.V == videoID) {
		result["success"] = true
		result["index"] = nextID
		app.playlist = append(app.playlist, videoID)
		app.playlistNames[videoID] = data.Name
		if app.connectedToPlayer {
			videoData := map[string]string{
				"id":    videoID,
				"title": data.Name,
			}
			app.playerConn.WriteJSON(videoData)
		}
	} else {
		result["success"] = false
	}

	marshalled, _ := json.Marshal(result)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write(marshalled)
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true }, //not checking origin
}

func (app *webapp) wsHandler(w http.ResponseWriter, r *http.Request) {
	conn, _ := upgrader.Upgrade(w, r, nil)
	app.playerConn = conn
	app.connectedToPlayer = true
	//push videos that have been proposed so far
	for _, videoID := range app.playlist {
		videoData := map[string]string{
			"id":    videoID,
			"title": app.playlistNames[videoID],
		}
		conn.WriteJSON(videoData)
	}
}


func queryYoutubeForTitle(id string) string {
	url := "https://www.googleapis.com/youtube/v3/search?part=snippet&type=video&q="+id+"&key=AIzaSyBE1vzq5FFFCn688oul-g1uFQOyWJhiWJY"

	title := ""
	response, err := http.Get(url)
	if err != nil {
	    fmt.Println(err)
	} else {
	    defer response.Body.Close()
		message := make(map[string]interface{})
	    decoder := json.NewDecoder(response.Body)
	    decoder.Decode(&message)

    	title = message["items"].([]interface{})[0].(map[string]interface{})["snippet"].(map[string]interface{})["title"].(string)
		fmt.Println(title)

	}
	return title
}

func main() {
	app := &webapp{
		playlist:          make([]string, 0),
		playlistNames:     make(map[string]string),
		paxosMap:          make(map[int]*rpc.Client),
		connectedToPlayer: false,
	}
	// Paxos init

	flag.Parse()
	paxosPortsString := *paxosPorts
	paxosPortsSlice := strings.Split(paxosPortsString, ",")

	//sleep till paxos nodes ready
	time.Sleep(time.Second * 2)

	var paxosCli *rpc.Client
	// Create RPC connection to paxos nodes.
	for i, p := range paxosPortsSlice {
		cli, err := rpc.DialHTTP("tcp", "localhost:"+p)
		if err != nil {
			fmt.Println("could not connect to node", i)
			continue
		}
		app.paxosMap[i] = cli
		paxosCli = cli
	}

	// check to see if there is already some saved state
	fmt.Println("checking for saved state")
	var getReply paxosrpc.GetValueReply
	listIdx := 1
	getArgs := &paxosrpc.GetValueArgs{Key: strconv.Itoa(listIdx)}
	for {
		err := paxosCli.Call("PaxosNode.GetValue", getArgs, &getReply)
		if err != nil {
			fmt.Println(err)
			break
		}
		if getReply.Status == paxosrpc.KeyNotFound {
			fmt.Println("no key")
			break
		}
		videoID := getReply.V.(string)
		app.playlist = append(app.playlist, videoID)
		listIdx++
		getArgs.Key = strconv.Itoa(listIdx)

		// HACK: requery youtube to get video name
		app.playlistNames[videoID] = queryYoutubeForTitle(videoID);

	}

	http.HandleFunc("/", app.indexHandler) // handler on initial landing page
	http.HandleFunc("/propose", app.proposeHandler)
	http.HandleFunc("/player", app.playerHandler)
	http.HandleFunc("/post_proposal", app.postProposalHandler)
	http.HandleFunc("/ws", app.wsHandler)
	// for serving js file that's referenced in html
	fs := http.FileServer(http.Dir("static"))
	http.Handle("/static/", http.StripPrefix("/static/", fs))
	http.ListenAndServe("localhost:8080", nil) // webserver running access http://localhost:8080/

	fmt.Println("Listening and serving...")
}
