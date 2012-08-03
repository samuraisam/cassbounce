package main

import (
	"cassbounce/server"
	"log"
)

func main() {
	shutdown := make(chan int)

	initial := []server.CassandraHost{server.CassandraHost{"0.0.0.0", 9160}}
	
	hostList := server.NewCassandraHostList(initial, true, shutdown)

	log.Print("host list ", hostList)

	server.Listen("0.0.0.0:9216")
}	
