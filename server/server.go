package server

import (
	"net"
	"log"
	"os"
	"github.com/pomack/thrift4go/lib/go/src/thrift"
	// "github.com/carloscm/gossie/src/cassandra"
)

// type CassHandler struct {

// }

// type 

type CassBouncer struct {
	conn net.Conn
	cassConn *CassConnection
	tsocket *thrift.TNonblockingSocket
	transport *thrift.TFramedTransport
	protocol thrift.TProtocol
}

func Listen(address string) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatal("error listening: ", err)
		os.Exit(1)
	}
	log.Print("listening on ", address)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Print("error accepting: ", err)
			return
		}
		log.Print("got client: ", conn)
		go func() {
			// cassConn, err := Dial("0.0.0.0:9160", "farty", 1000)
			// if err != nil {
			// 	log.Print("could not connect to cassandra node: ", err)
			// 	conn.Close()
			// 	return
			// }
			sock, _ := thrift.NewTNonblockingSocketConn(conn) // err is always nil, so just ignore
			trans := thrift.NewTFramedTransport(sock)
			protoFac := thrift.NewTBinaryProtocolFactoryDefault()
			prot := protoFac.GetProtocol(trans)
			bouncer := &CassBouncer{conn, nil, sock, trans, prot}
			bouncer.Run()
		}()
	}
}

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
func (b *CassBouncer) Run() {
	defer func() {
		//b.conn.Close()
		b.transport.Close()
		b.cassConn.Close()
	}()

	dead := false

	for !dead {
		// inspect the message, see if it's one we are interested in
		name, _, _, e := b.protocol.ReadMessageBegin()
		if e != nil {
			log.Print("error reading from client: ", e)
			return
		}
		log.Print("got name: ", name)
		return

		readBuf := make([]byte, 1024)
		n, err := b.conn.Read(readBuf)
		if err != nil {
			log.Print("error reading from client: ", err)
			return
		}
		log.Print("received ", n, " bytes of data")

		// var packet Buffer
		// packet.Write()

		n, cassWriteErr := b.cassConn.Write(readBuf[:n])
		if cassWriteErr != nil {
			log.Print("error writing to cassandra: ", cassWriteErr)
			return
		}
		log.Print("wrote ", n, " bytes of data to cassandra")

		cassReadBuf := make([]byte, 1024)
		n, cassReadErr := b.cassConn.Read(cassReadBuf)
		if cassReadErr != nil {
			log.Print("error reading from cassandra: ", cassReadErr)
			return
		}
		log.Print("read ", n, " bytes from cassandra")

		n, clientWriteErr := b.conn.Write(cassReadBuf[:n])
		if clientWriteErr != nil {
			log.Print("error sending reply: ", clientWriteErr)
		}
		log.Print("wrote ", n, " bytes of data")
	}
}