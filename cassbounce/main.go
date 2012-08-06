package main

import (
	"cassbounce/server"
	"flag"
	"log"
	"os"
	"strconv"
	"strings"
)

const (
	DEFAULT_PORT = 9160
)

var (
	initialServerList = flag.String("initial-servers", "0.0.0.0:9160", "A list of cassandra nodes to connect to initially.")
	nodeAutodiscovery = flag.Bool("node-autodiscovery", false, "Whether or not to introspect the ring to discover new nodes")
	listenAddress     = flag.String("listen-address", "0.0.0.0:9666", "What interface to listen on.")
	pollServers       = flag.Bool("perform-health-checks", false, "Whether or not to perform health checks on remote hosts")
)

func parseInitialServerList(s string) []server.CassandraHost {
	p := strings.Split(s, ",")
	ret := make([]server.CassandraHost, len(p))
	for i, el := range p {
		parts := strings.Split(el, ":")
		port := 0
		var err error
		if len(parts) == 1 { // no port was specified, default = 9160
			port = DEFAULT_PORT
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
	flag.Parse()

	// global shutdown signal - a number is sent over when services should shut down
	shutdown := make(chan int)

	// settings
	settings := server.NewAppSettings()
	// settings.InitialServerList = initial
	settings.NodeAutodiscovery = *nodeAutodiscovery
	settings.ListenAddress = *listenAddress
	settings.PollServersForever = *pollServers

	// global app
	app := server.GetApp()
	app.ShutdownChan = shutdown
	app.SetSettings(settings)

	// host list set up (this must happen after the global app object is set up)
	initial := parseInitialServerList(*initialServerList)
	// this will block until at least one successful connection can be established
	outboundHostList := server.NewCassandraHostList(initial, *nodeAutodiscovery, shutdown)
	app.SetHostList(outboundHostList)

	// listen forever
	err := app.Listen()

	if err != nil {
		log.Fatal("Could not listen on interface: ", app.Settings().ListenAddress, " error: ", err)
		os.Exit(1)
	}
}
