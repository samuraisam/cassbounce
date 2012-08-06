package server

import (
	"github.com/pomack/thrift4go/lib/go/src/thrift"
	"log"
	"net"
)

type Receiver interface {
	Receive()
}

type CommandReceiver struct {
	remoteConn net.Conn
	hostList   HostList
	poolMan    PoolManager
	tsocket    *thrift.TNonblockingSocket
	transport  *thrift.TFramedTransport
	protocol   thrift.TProtocol
}

func NewCommandReceiver(conn net.Conn, hostList HostList, poolMan PoolManager) *CommandReceiver {
	sock, _ := thrift.NewTNonblockingSocketConn(conn) // err is always nil, so just ignore it
	trans := thrift.NewTFramedTransport(sock)
	protoFac := thrift.NewTBinaryProtocolFactoryDefault()
	prot := protoFac.GetProtocol(trans)

	cmr := &CommandReceiver{}
	cmr.remoteConn = conn
	cmr.hostList = hostList
	cmr.poolMan = poolMan
	cmr.tsocket = sock
	cmr.transport = trans
	cmr.protocol = prot

	return cmr
}

func (r *CommandReceiver) Receive() {
	name, _, _, e := r.protocol.ReadMessageBegin()
	if e != nil {
		log.Print("CommandReceiver:Receive error reading from client: ", e)
	}
	log.Print("XXX: got name: ", name)
	return
}
