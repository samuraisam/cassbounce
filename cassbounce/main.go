package main

import (
	"fmt"
	"cassbounce/server"
	"flag"
	"log"
	"net"
	"os"
)

var testClient = flag.Bool("ping", false, "Set this to run a test client and ping a bouncer")

func main() {
	fmt.Println("I really sharted this time.")
	// conn, err := server.Dial("127.0.0.1:9160", "farts", 1000)
	flag.Parse()
	if !*testClient {
		server.Listen("0.0.0.0:9216")
	}

	conn, err := net.Dial("tcp", "0.0.0.0:9216")
	if err != nil {
		log.Fatal("could not connect to bouncer: ", err)
		os.Exit(1)
	}
	conn.Write([]byte("test"))

	b := make([]byte, 1024)
	n, err := conn.Read(b)
	if err != nil {
		log.Fatal("error reading: ", err)
		os.Exit(1)
	}
	log.Print("received ", n, " bytes --> ", string(b))
	// fmt.Println("like this: ", conn)
	// fmt.Println("nah like this: ", err)
}