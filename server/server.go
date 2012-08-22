package server

import (
	"log"
	"net"
	"time"
)

/*
 * AppSettings - global configuration
 */

type AppSettings struct {
	InitialServerList            []Host        // list of hosts that was passed into the command line
	NodeAutodiscovery            bool          // whether or not to set the HostList to autodiscover
	ListenAddress                string        // what interface to listen for new clients on
	ReceiverType                 string        // what receiver to use (for now, must only be "command")
	PoolManagerType              string        // what pool manager to use (for now, must only be "simple")
	PollServersForever           bool          // whether or not to constantly perform health checks on remote hosts
	PoolRecycleDuration          time.Duration // how frequently to recycle connections to upstream hosts
	PoolRecycleJitter            int           // jitter applied to recycle algorithm so all connections aren't closed at once
	PoolSize                     int           // maximum number of connections maintained to a given upstream server
	PoolConnectionAcquireTimeout time.Duration // wait this long to acquire a connection from a pool
	PoolConnectionAcquireRetries int           // number of times to endure connection aquisition failure before returning error to client
}

func NewAppSettings() *AppSettings {
	return &AppSettings{
		ReceiverType:                 "command",
		PoolManagerType:              "simple",
		NodeAutodiscovery:            false,
		ListenAddress:                "0.0.0.0:9160",
		PollServersForever:           false,
		PoolRecycleDuration:          time.Duration(60) * time.Second,
		PoolRecycleJitter:            10,
		PoolSize:                     10,
		PoolConnectionAcquireTimeout: time.Duration(100) * time.Millisecond,
		PoolConnectionAcquireRetries: 2,
	}
}

/*
 * App - the global app object
 *
 * Listens for new clients and holds global state
 *
 * Also manages stat collection (to be implemented)
 */

type App struct {
	hostList     HostList     // host list service - manages a list of up/down outbound servers
	settings     *AppSettings // global settings used throughout the app
	ShutdownChan chan int     // channel used when we want to shut down services
	poolMan      PoolManager  // pool manager service - manages a list outbound connection pools
}

// the global app object
var currentApp *App

func GetApp() *App {
	// app is a singleton, meaning we can only have one instance
	if currentApp != nil {
		// app was already created, return that one
		return currentApp
	}
	// create a new one since it didn't exist already
	currentApp = &App{}
	return currentApp
}

// TODO: this could respond to some environmental change
func (a *App) SetSettings(s *AppSettings) { a.settings = s }
func (a *App) Settings() *AppSettings     { return a.settings }

func (a *App) SetHostList(h HostList) { a.hostList = h }
func (a *App) HostList() HostList     { return a.hostList }

func (a *App) poolManager() PoolManager { // lazily load a PoolManager
	if a.poolMan == nil {
		switch a.settings.PoolManagerType {
		case "simple":
		default:
			a.poolMan = NewSimplePoolManager()
		}
	}
	return a.poolMan
}

// listen forever on the interface a.ListenAddress
func (a *App) Listen() error {
	listener, err := net.Listen("tcp", a.Settings().ListenAddress)
	if err != nil {
		return err
	}
	log.Print("App:Listen now accepting connections on interface: ", a.settings.ListenAddress)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Print("App:Listen error accepting connection: ", err)
			continue
		}
		// create a receiver for this connection
		var receiver Receiver
		rtype := a.settings.ReceiverType

		if rtype == "command" {
			receiver = NewCommandReceiver(conn, a.hostList, a.poolManager())
		}

		go receiver.Receive()
	}
	return nil
}

// type CassBouncer struct {
// 	conn      net.Conn
// 	cassConn  *CassConnection
// 	tsocket   *thrift.TNonblockingSocket
// 	transport *thrift.TFramedTransport
// 	protocol  thrift.TProtocol
// }

// func Listen(address string) {
// 	listener, err := net.Listen("tcp", address)
// 	if err != nil {
// 		log.Fatal("error listening: ", err)
// 		os.Exit(1)
// 	}
// 	log.Print("listening on ", address)
// 	for {
// 		conn, err := listener.Accept()
// 		if err != nil {
// 			log.Print("error accepting: ", err)
// 			return
// 		}
// 		log.Print("got client: ", conn)
// 		go func() {
// 			// cassConn, err := Dial("0.0.0.0:9160", "farty", 1000)
// 			// if err != nil {
// 			// 	log.Print("could not connect to cassandra node: ", err)
// 			// 	conn.Close()
// 			// 	return
// 			// }
// 			sock, _ := thrift.NewTNonblockingSocketConn(conn) // err is always nil, so just ignore
// 			trans := thrift.NewTFramedTransport(sock)
// 			protoFac := thrift.NewTBinaryProtocolFactoryDefault()
// 			prot := protoFac.GetProtocol(trans)
// 			bouncer := &CassBouncer{conn, nil, sock, trans, prot}
// 			bouncer.Run()
// 		}()
// 	}
// }

/*
The goal of this thing is to offer a connection pooler for cassandra

NOTES

 - a client should not maintain a connection pool of its own. if this is a web request in a synchronous
 language like python (and, for example, the django framework, which is also synchronous) then you would
 open up a connection at the beginning of every request, and kill the connection at the end, in
 exactly the same way that postgres conections are handeled

 - in that vein (the way postgres connections are handeled in django, for example) we want to treat
 cassandra the same way (1 connection per request, disconnect at the end)

	1. it enforces the "shared nothing" architecture of a web worker. we don't want to have a shared
	connection pool in the web worker, as it will be shared amongst threads and processes

	2. makes it easier to reason about the various parallellism methods python implements in web
	processes. don't worry about sharing a connection pool with your preforkers, for example

	3. we can connect to a local connection pooler which *is* good at things like concurrency and
	paralellism to handle the work of maintaining connections (state, shared stuff) for us

 - each connection has a keyspace assigned to it. this means we need to inspect the packets somewhat
 to determine which keyspace to use for each connection. other than that we'll just send stuff
 directly through to cassandra

TODO

	 - when a client connects and asks for a keyspace, if we have a connection that already has 
	 the keyspace, then pull one from the pool and use that as the connection by:

	 	+ checkout a connection with the keyspace
	 	+ send back to the client the keyspace result

	 - if a client connects and asks for a keyspace that is not yet in the pool, create one by:

		+ connect the socket
		+ parse the result of the client packet and get the keyspace string
		+ call set_keyspace with the keyspace string
		+ cache the binary result so it may be sent back in the future

*/
// func (b *CassBouncer) Run() {
// 	defer func() {
// 		//b.conn.Close()
// 		b.transport.Close()
// 		b.cassConn.Close()
// 	}()

// 	dead := false

// 	for !dead {
// 		// inspect the message, see if it's one we are interested in
// 		name, _, _, e := b.protocol.ReadMessageBegin()
// 		if e != nil {
// 			log.Print("error reading from client: ", e)
// 			return
// 		}
// 		log.Print("got name: ", name)
// 		return

// 		readBuf := make([]byte, 1024)
// 		n, err := b.conn.Read(readBuf)
// 		if err != nil {
// 			log.Print("error reading from client: ", err)
// 			return
// 		}
// 		log.Print("received ", n, " bytes of data")

// 		// var packet Buffer
// 		// packet.Write()

// 		n, cassWriteErr := b.cassConn.Write(readBuf[:n])
// 		if cassWriteErr != nil {
// 			log.Print("error writing to cassandra: ", cassWriteErr)
// 			return
// 		}
// 		log.Print("wrote ", n, " bytes of data to cassandra")

// 		cassReadBuf := make([]byte, 1024)
// 		n, cassReadErr := b.cassConn.Read(cassReadBuf)
// 		if cassReadErr != nil {
// 			log.Print("error reading from cassandra: ", cassReadErr)
// 			return
// 		}
// 		log.Print("read ", n, " bytes from cassandra")

// 		n, clientWriteErr := b.conn.Write(cassReadBuf[:n])
// 		if clientWriteErr != nil {
// 			log.Print("error sending reply: ", clientWriteErr)
// 		}
// 		log.Print("wrote ", n, " bytes of data")
// 	}
// }
