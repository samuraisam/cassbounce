package server

import (
	"errors"
	"github.com/carloscm/gossie/src/cassandra"
	"github.com/pomack/thrift4go/lib/go/src/thrift"
	"log"
	"net"
	"time"
)

type Connection interface {
	Write(buf []byte) (nwritten int, err error)
	Read(buf []byte) (nread int, err error)
	Close() (err error)
}

type CassConnection struct {
	socket          *thrift.TNonblockingSocket
	transport       *thrift.TFramedTransport
	client          *cassandra.CassandraClient
	node            string
	keyspace        string
	protocolFactory *thrift.TBinaryProtocolFactory
}

var (
	ErrorConnectionTimeout = errors.New("Connection timeout")
)

/*

TODO:

	- on connect: set_keyspace

*/
func Dial(node string, keyspace string, timeout time.Duration) (*CassConnection, error) {
	// resolve the address 
	addr, err := net.ResolveTCPAddr("tcp", node)
	if err != nil {
		return nil, err
	}
	// get a socket
	sock, err := thrift.NewTNonblockingSocketAddr(addr)
	if err != nil {
		return nil, err
	}
	sock.SetTimeout(int64(timeout))

	// build the client
	transport := thrift.NewTFramedTransport(sock)
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	cli := cassandra.NewCassandraClientFactory(transport, protocolFactory)

	// connect, with a timeout of `timeout`
	ch := make(chan bool, 1)
	go func() {
		err = transport.Open()
		ch <- true
	}()
	timedOut := false
	select {
	case <-time.After(timeout):
		timedOut = true
	case <-ch:
	}
	if timedOut {
		return nil, ErrorConnectionTimeout
	}
	// get the version and check for min version and what not
	ver, err := cli.DescribeVersion()
	if err != nil {
		transport.Close()
		return nil, err
	}
	log.Print("Opened new connection, protocol version: ", ver)
	return &CassConnection{sock, transport, cli, node, keyspace, protocolFactory}, nil
}

func (c *CassConnection) Write(buf []byte) (int, error) {
	return c.socket.Write(buf)
}

func (c *CassConnection) Read(buf []byte) (int, error) {
	return c.socket.Read(buf)
}

func (c *CassConnection) Close() error {
	return c.transport.Close()
}
