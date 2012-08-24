package main

import (
	"cassbounce/server"
	"cassbounce/server/config"
	"flag"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	DEFAULT_PORT = 9160
)

var (
	// server options
	listenAddress = flag.String("listen-address", "0.0.0.0:9666", "What interface to listen on")

	// host list options
	initialServerList = flag.String("initial-servers", "0.0.0.0:9160", "A list of cassandra nodes to connect to initially")
	nodeAutodiscovery = flag.Bool("node-autodiscovery", false, "Whether or not to introspect the ring to discover new nodes")
	pollServers       = flag.Bool("perform-health-checks", false, "Whether or not to perform health checks on remote hosts")

	// pool options
	poolRecycleDuration          = flag.Int("pool-recycle-duration", 60, "After this many seconds, the pool may automatically recycle (close) a connection")
	poolRecycleJitter            = flag.Int("pool-recycle-jitter", 10, "Max jitter to add to recycle proceure so that not all connections are closed at the same time")
	poolSize                     = flag.Int("pool-size", 10, "Maximum number of connections a pool will open up to a given node")
	poolConnectionAcquireTimeout = flag.Int("pool-connection-acquire-timeout", 100, "Max number of milliseconds to wait to acquire a connection before retrying or giving up")
	poolConnectionAcquireRetries = flag.Int("pool-connection-acquire-retries", 2, "Max number of retries to acquire a connection before giving up. (resulting from an error or a timeout)")
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
	settings := config.NewAppSettings()
	// settings.InitialServerList = initial
	settings.NodeAutodiscovery = *nodeAutodiscovery
	settings.ListenAddress = *listenAddress
	settings.PollServersForever = *pollServers
	settings.PoolRecycleDuration = time.Duration(*poolRecycleDuration) * time.Second
	settings.PoolRecycleJitter = *poolRecycleJitter
	settings.PoolSize = *poolSize
	settings.PoolConnectionAcquireTimeout = time.Duration(*poolConnectionAcquireTimeout) * time.Millisecond
	settings.PoolConnectionAcquireRetries = *poolConnectionAcquireRetries

	config.Get().SetSettings(settings)

	// global app
	app := server.New()
	app.ShutdownChan = shutdown

	// host list set up (this must happen after the global app object is set up)
	initial := parseInitialServerList(*initialServerList)
	// this will block until at least one successful connection can be established
	outboundHostList := server.NewCassandraHostList(initial, *nodeAutodiscovery, shutdown)
	app.SetHostList(outboundHostList)

	// listen forever
	err := app.Listen()

	if err != nil {
		log.Fatal("Could not listen on interface: ", settings.ListenAddress, " error: ", err)
		os.Exit(1)
	}
}
