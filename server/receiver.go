package server

import (
	"bytes"
	"cassbounce/server/cassutils"
	"cassbounce/server/thriftutils"
	"errors"
	"fmt"
	"github.com/carloscm/gossie/src/cassandra"
	"github.com/pomack/thrift4go/lib/go/src/thrift"
	"log"
	"net"
	"time"
)

type Receiver interface {
	Receive()
}

/*
 * CommandReceiver
 *
 * A receiver which pulls full commands off the wire one at a time, relaying them to
 * a chosen cassandra backend. Attempts to do as little processing as possible.
 */
type CommandReceiver struct {
	remoteConn net.Conn
	hostList   HostList
	poolMan    PoolManager
	tsocket    *thrift.TNonblockingSocket
	transport  thrift.TTransport
	protocol   thrift.TProtocol
}

func NewCommandReceiver(conn net.Conn, hostList HostList, poolMan PoolManager) *CommandReceiver {
	sock, _ := thrift.NewTNonblockingSocketConn(conn) // err is always nil, so just ignore it
	trans := thriftutils.NewTFramedTransport(sock)
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

/*
 * Begin receiving from an inbound connection, and continue receiving from it until the connection is severed.
 */
func (r *CommandReceiver) Receive() {
	// XXX: get a server connection - temporary
	destinationHost, err := r.hostList.Get()
	if err != nil {
		log.Print("XXX: error getting outbound host: ", err)
		return
	}
	outboundConn, err := Dial(destinationHost.String(), "fart", time.Duration(100)*time.Millisecond)
	if err != nil {
		log.Print("XXX: Error establishing outbound connection: ", err)
		return
	}

	defer func() { r.remoteConn.Close(); outboundConn.Close() }()

	dead := false

	var connDef *ConnectionDef = nil
	// connDef = nil

	for !dead {
		newConnDef, cmd, err := r.Handshake() // perform the handshake, this will establish keyspace and login info if needed

		// got a connection error, disconnect client
		if err != nil {
			dead = true
			continue
		}

		// handshake failed, or didn't get a command successfully, not necessary to disconnect client
		if cmd == nil {
			continue
		}
		// if cmd.Name() == "get_slice" {
		// 	rp, _ := cassutils.NewArgsRetainingProcessor(cmd.Name())
		// 	succ, exc := rp.ReadArgs(r.protocol)
		// 	if !succ {
		// 		log.Print("XXX: error reading args: ", exc)
		// 	}
		// }
		if connDef == nil || (newConnDef.Dirty() && !connDef.Equals(newConnDef)) {
			// get a new connection
			log.Print("Getting a new connection: ", newConnDef)
			connDef = newConnDef
			outboundConn.client.SetKeyspace(newConnDef.Keyspace())
			outboundConn.client.Login(newConnDef.LoginReq().GetCassAuthRequest())
		}

		err = cmd.SetStream(TTransportReadGen(r.transport, "inboundReq"))
		if err != nil {
			continue // TODO: notify internal error
		}

		res := cmd.Execute(outboundConn, time.Duration(100)*time.Millisecond)

		// res := <-r.poolManager.Do(connDef, func (conn *Connection) {
		// 	return cmd.Execute(TTransportReadGen(r.transport, "inboundReq"), conn, time.Duration(100)*time.Millisecond)
		// });

		failureWriting := false
		for pkt := range res {
			// write the response to the client
			if pkt.Error() != nil {
				log.Print("CommandReceiver:Receive error reading response from upstream client: ", pkt.Error())
				return
			}
			_, rErr := r.transport.Write(pkt.Bytes())
			if rErr != nil {
				log.Print("CommandReceiver:Receive error sending data back to client: ", rErr)
				failureWriting = true
				break
			}
		}

		if failureWriting {
			break
		}

		fErr := r.transport.Flush()
		if fErr != nil {
			log.Print("CommandReceiver:Receive error flushing data to client: ", fErr)
			break
		}
	}
}

/*
 * Begin reading the connection and establish and allocate a Command and ConnectionDef
 *
 * It will intercept `set_keyspace` and `login` and use those to populate a ConnectionDef if sent by a client
 */

func (r *CommandReceiver) Handshake() (*ConnectionDef, Command, error) {
	// start reading immediatly; get the header
	name, typeId, seqId, err := r.protocol.ReadMessageBegin()
	if err != nil {
		log.Print("CommandReceiver:Handshake could not read from client ", err)
		r.protocol.Skip(thrift.STRUCT)
		r.protocol.ReadMessageEnd()
		TWriteException(name, seqId, r.protocol, err)
		return nil, nil, err
	}

	var lastHandhakeStep string // set if handshaking, used to generate useful logging output
	connDef := NewEmptyConnectionDef()

	// if we are handshaking, that is name is either `set_keyspace` or `login` then 
	for name == "set_keyspace" || name == "login" {
		lastHandhakeStep = name
		// set up the passthrough processor
		pt := NewLoginAndKeyspaceHijackingHandler()
		proc := cassandra.NewCassandraProcessor(pt)

		// get a processor for the name
		processor, nameFound := proc.GetProcessorFunction(name)

		if !nameFound || processor == nil {
			// have no idea what you are tryn'a do son (shouldn't ever get here as we provide the processor)
			exc := thrift.NewTApplicationException(thrift.UNKNOWN_METHOD, "Unknown function "+name)
			r.protocol.Skip(thrift.STRUCT)
			r.protocol.ReadMessageEnd()
			TWriteException(name, seqId, r.protocol, exc)
			return nil, nil, nil // nil err = did not have a connection error
		}

		// run it
		_, exc := processor.Process(seqId, r.protocol, r.protocol)
		if exc != nil {
			log.Print("CommandReceiver:Receiver failed to execute function '", name, "': ", exc)
			TWriteException(name, seqId, r.protocol, exc)
			return nil, nil, nil // nil err = did not have a connection error
		}

		if pt.HasLogin() {
			connDef.SetLoginReq(NewLoginReq(pt.Username(), pt.Password()))
		}
		if pt.HasKeyspace() {
			connDef.SetKeyspace(pt.Keyspace())
		}

		name, typeId, seqId, err = r.protocol.ReadMessageBegin() // start reading the next message to get the command

		if err != nil {
			log.Print("CommandReceiver:Handshake could not read message header from client after handshake step '", lastHandhakeStep, "' ", err)
			r.protocol.Skip(thrift.STRUCT)
			r.protocol.ReadMessageEnd()
			TWriteException(name, seqId, r.protocol, err)
			return nil, nil, err // bail out totally
		}
	}

	// we are not handshaking, just executing commands
	return connDef, NewCassandraCommand(name, typeId, seqId), nil
}

/*
 * A TTransport used to just pass all data written to it into the provided bytes.Buffer
 */
type PassthroughTransport struct {
	buffer bytes.Buffer
}

func (t *PassthroughTransport) IsOpen() bool { return true }
func (t *PassthroughTransport) Open() error  { return nil }
func (t *PassthroughTransport) Close() error {
	t.buffer.Reset()
	return nil
}
func (t *PassthroughTransport) ReadAll(buf []byte) (int, error) {
	r, e := t.Read(buf)
	if r != len(buf) {
		return 0, errors.New(fmt.Sprintf("All of %d could not be read!", r))
	}
	return r, e
}
func (t *PassthroughTransport) Read(buf []byte) (int, error) {
	// log.Print("reading ")
	return t.buffer.Read(buf)
}
func (t *PassthroughTransport) Write(buf []byte) (int, error) {
	// log.Print("writing ", string(buf))
	return t.buffer.Write(buf)
}
func (t *PassthroughTransport) Flush() error { return nil }
func (t *PassthroughTransport) Peek() bool   { return false }

/*
LoginAndKeyspaceHijackingHandler pretends to be a CassandraProcessor and intercepts `login` and `set_keyspace`, 
assigning the values to itself.
*/
type LoginAndKeyspaceHijackingHandler struct {
	*cassutils.PassthroughHandler
	keyspace    string
	username    string
	password    string
	hasLogin    bool
	hasKeyspace bool
}

func NewLoginAndKeyspaceHijackingHandler() *LoginAndKeyspaceHijackingHandler {
	return &LoginAndKeyspaceHijackingHandler{&cassutils.PassthroughHandler{}, "", "", "", false, false}
}

func (c *LoginAndKeyspaceHijackingHandler) Keyspace() string  { return c.keyspace }
func (c *LoginAndKeyspaceHijackingHandler) HasKeyspace() bool { return c.hasKeyspace }
func (c *LoginAndKeyspaceHijackingHandler) HasLogin() bool    { return c.hasLogin }
func (c *LoginAndKeyspaceHijackingHandler) Username() string  { return c.username }
func (c *LoginAndKeyspaceHijackingHandler) Password() string  { return c.password }

/*
 * Overrides the `login` thrift method - it has no usable return value so just return nil
 */
func (c *LoginAndKeyspaceHijackingHandler) Login(authReq *cassandra.AuthenticationRequest) (*cassandra.AuthenticationException, *cassandra.AuthorizationException, error) {
	un, uer := authReq.Credentials.Get("username")
	pw, per := authReq.Credentials.Get("password")

	if uer && per {
		c.hasLogin = true
		c.username = un.(string)
		c.password = pw.(string)
	}

	return nil, nil, nil
}

/*
 * Overrides the `set_keyspace` thrift method - it has no usable return value, so just return nil
 */
func (c *LoginAndKeyspaceHijackingHandler) SetKeyspace(keyspace string) (*cassandra.InvalidRequestException, error) {
	c.hasKeyspace = true
	c.keyspace = keyspace
	return nil, nil
}
