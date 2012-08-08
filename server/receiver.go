package server

import (
	"bytes"
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

	for !dead {
		// read the header - this basically does the exact same thing as proc.Process(in_prot, out_prot)
		name, typeId, seqId, err := r.protocol.ReadMessageBegin()
		if err != nil {
			log.Print("CommandReceiver:Receiver could not read from client ", err)
			dead = true
			continue // TODO: try to send an exception
		}

		// if it's something we're interested in, intercept it and process it using our own handler
		if name == "set_keyspace" || name == "login" {
			// set up the passthrough processor
			pt := &CassandraPassthrough{outboundConn}
			proc := cassandra.NewCassandraProcessor(pt)

			processor, nameFound := proc.GetProcessorFunction(name) // get a processor for the name

			if !nameFound || processor == nil {
				// have no idea what you are tryn'a do son
				r.protocol.Skip(thrift.STRUCT)
				r.protocol.ReadMessageEnd()
				exc := thrift.NewTApplicationException(thrift.UNKNOWN_METHOD, "Unknown function "+name)
				r.protocol.WriteMessageBegin(name, thrift.EXCEPTION, seqId)
				exc.Write(r.protocol)
				r.protocol.WriteMessageEnd()
				r.protocol.Transport().Flush()
				dead = true
				continue
			}

			_, exc := processor.Process(seqId, r.protocol, r.protocol)
			if exc != nil {
				log.Print("CommandReceiver:Receiver failed to execute function '", name, "': ", exc)
				dead = true
			}
			continue // move to next whether or not this was successful
		}

		// fart
		cmd := NewCassandraCommand(name, typeId, seqId)
		<-cmd.Execute(/*readGen(r.transport, "inboundReq"),*/ outboundConn, time.Duration(100) * time.Millisecond, r.protocol)
		log.Print("herp")
		continue

		// for pkt := range res {
		// 	// write the response to the client
		// 	if pkt.Error() != nil {
		// 		log.Print("CommandReceiver:Receive error reading response from upstream client: ", pkt.Error())
		// 		return
		// 	}
		// 	log.Print("CommandReceiver:Receive writing ", pkt.Len(), " bytes: ", string(pkt.Bytes()))
		// 	rWritten, rErr := r.transport.Write(pkt.Bytes())
		// 	if rErr != nil {
		// 		log.Print("CommandReceiver:Receive error sending data back to client: ", rErr)
		// 		break
		// 	}
		// 	log.Print("CommandReceiver:Receive wrote ", rWritten, " bytes to client")
		// }
		// er := r.transport.Flush()
		// log.Print("flushed! ", er)

		// outboundConn.protocol.WriteMessageBegin(name, typeId, seqId) // restore the message header
		inboundData := make([]byte, 1024)           // rest of the data coming off the inbound connection
		l, readErr := r.transport.Read(inboundData) // read the rest of the inbound data
		if readErr != nil {
			log.Print("error reading inbound data: ", readErr) // TODO: notify client
			dead = true
			continue
		}
		log.Print("got inbound name: ", name, " data: ", string(inboundData[:l]))

		// write the outbound request
		outboundProt := outboundConn.protocolFactory.GetProtocol(outboundConn.transport) // create a protocol with the outbound transport
		outboundProt.WriteMessageBegin(name, typeId, seqId)                              // write the header to the protocol (which writes it to the transport)
		outboundProt.Transport().Write(inboundData)                                      // write the remaining data to the outbound transport
		outboundProt.Transport().Flush()

		// read the outbound response
		oName, oTypeId, oSeqId, oErr := outboundProt.ReadMessageBegin()
		if oSeqId != seqId {
			// hmm, got the wrong response back
			r.protocol.WriteMessageBegin(name, thrift.EXCEPTION, seqId)
			exc := thrift.NewTApplicationException(thrift.BAD_SEQUENCE_ID, "unmatched seqIds returned in response from downstream server")
			exc.Write(r.protocol)
			r.protocol.WriteMessageEnd()
			r.protocol.Transport().Flush()
			dead = true
			continue
		}

		if oErr != nil {
			log.Print("error reading response header from upstream server ", oErr) // TODO: notify client
			dead = true
			continue
		}

		// read the rest of the outbound response
		outboundData := make([]byte, 1024)
		l, oReadEr := outboundProt.Transport().Read(outboundData)
		if oReadEr != nil {
			log.Print("error reading response from upstream server")
			dead = true
			continue // TODO: notify client
		}

		log.Print("read ", l, " bytes response from upstream server")

		// write the response to the inbound connection
		r.protocol.WriteMessageBegin(oName, oTypeId, oSeqId)
		_, iErr := r.protocol.Transport().Write(outboundData[:l])
		r.protocol.Transport().Flush()

		if iErr != nil {
			log.Print("error writing response back to inbound connection: ", iErr)
		}
	}
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
 * CassandraPassthrough
 *
 * Implements `ICassandra` so we may hijack some communications with the backend servers
 *
 * Most things are not implemented
 */
type CassandraPassthrough struct {
	conn *CassConnection
}

func (c *CassandraPassthrough) Login(auth_request *cassandra.AuthenticationRequest) (authnx *cassandra.AuthenticationException, authzx *cassandra.AuthorizationException, err error) {
	k1, _ := auth_request.Credentials.Get("username")
	k2, _ := auth_request.Credentials.Get("password")
	log.Print("CassandraPassthrough:Login  username=", k1, " password=", k2)
	//return c.conn.client.Login(auth_request)
	return nil, nil, nil
}

func (c *CassandraPassthrough) SetKeyspace(keyspace string) (ire *cassandra.InvalidRequestException, err error) {
	log.Print("CassandraPassthrough:SetKeyspace ks=", keyspace)
	return c.conn.client.SetKeyspace(keyspace)
}

func (c *CassandraPassthrough) Get(key []byte, column_path *cassandra.ColumnPath, consistency_level cassandra.ConsistencyLevel) (retval448 *cassandra.ColumnOrSuperColumn, ire *cassandra.InvalidRequestException, nfe *cassandra.NotFoundException, ue *cassandra.UnavailableException, te *cassandra.TimedOutException, err error) {
	return nil, nil, nil, nil, nil, nil
}

func (c *CassandraPassthrough) GetSlice(key []byte, column_parent *cassandra.ColumnParent, predicate *cassandra.SlicePredicate, consistency_level cassandra.ConsistencyLevel) (retval449 thrift.TList, ire *cassandra.InvalidRequestException, ue *cassandra.UnavailableException, te *cassandra.TimedOutException, err error) {
	return nil, nil, nil, nil, nil
}

func (c *CassandraPassthrough) GetCount(key []byte, column_parent *cassandra.ColumnParent, predicate *cassandra.SlicePredicate, consistency_level cassandra.ConsistencyLevel) (retval450 int32, ire *cassandra.InvalidRequestException, ue *cassandra.UnavailableException, te *cassandra.TimedOutException, err error) {
	return 0, nil, nil, nil, nil
}

func (c *CassandraPassthrough) MultigetSlice(keys thrift.TList, column_parent *cassandra.ColumnParent, predicate *cassandra.SlicePredicate, consistency_level cassandra.ConsistencyLevel) (retval451 thrift.TMap, ire *cassandra.InvalidRequestException, ue *cassandra.UnavailableException, te *cassandra.TimedOutException, err error) {
	return nil, nil, nil, nil, nil
}

func (c *CassandraPassthrough) MultigetCount(keys thrift.TList, column_parent *cassandra.ColumnParent, predicate *cassandra.SlicePredicate, consistency_level cassandra.ConsistencyLevel) (retval452 thrift.TMap, ire *cassandra.InvalidRequestException, ue *cassandra.UnavailableException, te *cassandra.TimedOutException, err error) {
	return nil, nil, nil, nil, nil
}

func (c *CassandraPassthrough) GetRangeSlices(column_parent *cassandra.ColumnParent, predicate *cassandra.SlicePredicate, range_a1 *cassandra.KeyRange, consistency_level cassandra.ConsistencyLevel) (retval453 thrift.TList, ire *cassandra.InvalidRequestException, ue *cassandra.UnavailableException, te *cassandra.TimedOutException, err error) {
	return nil, nil, nil, nil, nil
}

func (c *CassandraPassthrough) GetIndexedSlices(column_parent *cassandra.ColumnParent, index_clause *cassandra.IndexClause, column_predicate *cassandra.SlicePredicate, consistency_level cassandra.ConsistencyLevel) (retval454 thrift.TList, ire *cassandra.InvalidRequestException, ue *cassandra.UnavailableException, te *cassandra.TimedOutException, err error) {
	return nil, nil, nil, nil, nil
}

func (c *CassandraPassthrough) Insert(key []byte, column_parent *cassandra.ColumnParent, column *cassandra.Column, consistency_level cassandra.ConsistencyLevel) (ire *cassandra.InvalidRequestException, ue *cassandra.UnavailableException, te *cassandra.TimedOutException, err error) {
	return nil, nil, nil, nil
}

func (c *CassandraPassthrough) Add(key []byte, column_parent *cassandra.ColumnParent, column *cassandra.CounterColumn, consistency_level cassandra.ConsistencyLevel) (ire *cassandra.InvalidRequestException, ue *cassandra.UnavailableException, te *cassandra.TimedOutException, err error) {
	return nil, nil, nil, nil
}

func (c *CassandraPassthrough) Remove(key []byte, column_path *cassandra.ColumnPath, timestamp int64, consistency_level cassandra.ConsistencyLevel) (ire *cassandra.InvalidRequestException, ue *cassandra.UnavailableException, te *cassandra.TimedOutException, err error) {
	return nil, nil, nil, nil
}

func (c *CassandraPassthrough) RemoveCounter(key []byte, path *cassandra.ColumnPath, consistency_level cassandra.ConsistencyLevel) (ire *cassandra.InvalidRequestException, ue *cassandra.UnavailableException, te *cassandra.TimedOutException, err error) {
	return nil, nil, nil, nil
}

func (c *CassandraPassthrough) BatchMutate(mutation_map thrift.TMap, consistency_level cassandra.ConsistencyLevel) (ire *cassandra.InvalidRequestException, ue *cassandra.UnavailableException, te *cassandra.TimedOutException, err error) {
	return nil, nil, nil, nil
}

func (c *CassandraPassthrough) Truncate(cfname string) (ire *cassandra.InvalidRequestException, ue *cassandra.UnavailableException, err error) {
	return nil, nil, nil
}

func (c *CassandraPassthrough) DescribeSchemaVersions() (retval461 thrift.TMap, ire *cassandra.InvalidRequestException, err error) {
	return nil, nil, nil
}

func (c *CassandraPassthrough) DescribeKeyspaces() (retval462 thrift.TList, ire *cassandra.InvalidRequestException, err error) {
	return nil, nil, nil
}

func (c *CassandraPassthrough) DescribeClusterName() (retval463 string, err error) { return "", nil }

func (c *CassandraPassthrough) DescribeVersion() (retval464 string, err error) { return "", nil }

func (c *CassandraPassthrough) DescribeRing(keyspace string) (retval465 thrift.TList, ire *cassandra.InvalidRequestException, err error) {
	return nil, nil, nil
}

func (c *CassandraPassthrough) DescribePartitioner() (retval466 string, err error) { return "", nil }

func (c *CassandraPassthrough) DescribeSnitch() (retval467 string, err error) { return "", nil }

func (c *CassandraPassthrough) DescribeKeyspace(keyspace string) (retval468 *cassandra.KsDef, nfe *cassandra.NotFoundException, ire *cassandra.InvalidRequestException, err error) {
	return nil, nil, nil, nil
}

func (c *CassandraPassthrough) DescribeSplits(cfName string, start_token string, end_token string, keys_per_split int32) (retval469 thrift.TList, ire *cassandra.InvalidRequestException, err error) {
	return nil, nil, nil
}

func (c *CassandraPassthrough) SystemAddColumnFamily(cf_def *cassandra.CfDef) (retval470 string, ire *cassandra.InvalidRequestException, sde *cassandra.SchemaDisagreementException, err error) {
	return "", nil, nil, nil
}

func (c *CassandraPassthrough) SystemDropColumnFamily(column_family string) (retval471 string, ire *cassandra.InvalidRequestException, sde *cassandra.SchemaDisagreementException, err error) {
	return "", nil, nil, nil
}

func (c *CassandraPassthrough) SystemAddKeyspace(ks_def *cassandra.KsDef) (retval472 string, ire *cassandra.InvalidRequestException, sde *cassandra.SchemaDisagreementException, err error) {
	return "", nil, nil, nil
}

func (c *CassandraPassthrough) SystemDropKeyspace(keyspace string) (retval473 string, ire *cassandra.InvalidRequestException, sde *cassandra.SchemaDisagreementException, err error) {
	return "", nil, nil, nil
}

func (c *CassandraPassthrough) SystemUpdateKeyspace(ks_def *cassandra.KsDef) (retval474 string, ire *cassandra.InvalidRequestException, sde *cassandra.SchemaDisagreementException, err error) {
	return "", nil, nil, nil
}

func (c *CassandraPassthrough) SystemUpdateColumnFamily(cf_def *cassandra.CfDef) (retval475 string, ire *cassandra.InvalidRequestException, sde *cassandra.SchemaDisagreementException, err error) {
	return "", nil, nil, nil
}

func (c *CassandraPassthrough) ExecuteCqlQuery(query []byte, compression cassandra.Compression) (retval476 *cassandra.CqlResult, ire *cassandra.InvalidRequestException, ue *cassandra.UnavailableException, te *cassandra.TimedOutException, sde *cassandra.SchemaDisagreementException, err error) {
	return nil, nil, nil, nil, nil, nil
}
