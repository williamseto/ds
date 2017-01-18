package main

import (
    "fmt"
    //"bufio"
    "net"
    "strconv"
)

const (
    defaultHost = "localhost"
    defaultPort = 9999
)

// To test your server implementation, you might find it helpful to implement a
// simple 'client runner' program. The program could be very simple, as long as
// it is able to connect with and send messages to your server and is able to
// read and print out the server's response to standard output. Whether or
// not you add any code to this file will not affect your grade.
func main() {
	addr := net.JoinHostPort(defaultHost, strconv.Itoa(defaultPort))
    conn, err := net.Dial("tcp", addr)
	if err != nil {
		// handle error
		return
	}
	fmt.Fprintf(conn, "put,hello,world\n")
	fmt.Fprintf(conn, "get,hello\n")

	//reader := bufio.NewReader(conn)
	for {
		 // fmt.Println("preparing to read")
		 // str, err := reader.ReadString('\n')

		 // if err != nil {
		 // 	break
		 // }
		 // fmt.Println("response: ", str)
	}

	fmt.Println("closing")
	conn.Close()
}
