package main

import (
	"cassbounce/server"
	"log"
	"flag"
	"strings"
	"os"
	"strconv"
)

var (
	initialServerList = flag.String("initial-servers", "0.0.0.0:9160", "A list of cassandra nodes to connect to initially.")
	nodeAutodiscovery = flag.Bool("node-autodiscovery", false, "Whether or not to introspect the ring to discover new nodes")
)

func parseInitialServerList(s string) []server.CassandraHost {
	p := strings.Split(s, ",")
	ret := make([]server.CassandraHost, len(p))
	for i, el := range p {
		parts := strings.Split(el, ":")
		port := 0
		var err error
		if len(parts) == 1 { // no port was specified
			port = 9160
		} else if len(parts) == 2 {
			port, err = strconv.Atoi(parts[1])
			if err != nil {
				log.Fatal("Error parsing host definition: ", err)
				os.Exit(1)
			}
		} else {
			log.Fatal("Invalid host defintion '", el, "' (should be in the format of HOST:PORT)")
			os.Exit(1)
		}
		ret[i] = server.NewCassandraHost(parts[0], port)
	}
	return ret
}

func main() {
	shutdown := make(chan int)

	flag.Parse()

	initial := parseInitialServerList(*initialServerList)
	hostList := server.NewCassandraHostList(initial, *nodeAutodiscovery, shutdown)

	log.Print("Host list ", hostList)

	server.Listen("0.0.0.0:9216")
}
