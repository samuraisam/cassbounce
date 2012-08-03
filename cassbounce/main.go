package main

import (
	"cassbounce/server"
	"log"
)

func main() {
	shutdown := make(chan int)

	initial := []server.CassandraHost{server.NewCassandraHost("0.0.0.0", 9160)}

	hostList := server.NewCassandraHostList(initial, true, shutdown)

	h, e := hostList.Get()

	log.Print("host list ", h, e)

	server.Listen("0.0.0.0:9216")
}
