package server

import (
	"bytes"
	"github.com/carloscm/gossie/src/cassandra"
	"github.com/pomack/thrift4go/lib/go/src/thrift"
	"log"
	"net"
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
	// name, _, _, e := r.protocol.ReadMessageBegin()
	// if e != nil {
	// 	log.Print("CommandReceiver:Receive error reading from client: ", e)
	// }
	// log.Print("XXX: got name: ", name)
	// return

	defer func() { r.remoteConn.Close() }()

	dead := false

	for !dead {
		// set up the passthrough processor
		pt := &CassandraPassthrough{}
		proc := cassandra.NewCassandraProcessor(pt)

		// output stubbing
		// protFac := thrift.NewTBinaryProtocolFactoryDefault()
		// var tbuf bytes.Buffer
		// trans := PassthroughTransport{tbuf}
		// oprot := protFac.GetProtocol(&trans)

		_, exc := proc.Process(r.protocol, r.protocol)
		if exc != nil {
			log.Print("XXX: Shit! ", exc)
			dead = true
			continue
		}

		// obuf := make([]byte, 1024)
		// l, _ := trans.Read(obuf)

		// // log.Print("XXX: writing ", l, " ", err, " ", string(obuf))

		// ll, eerr := r.remoteConn.Write(obuf[:l])
		// log.Print("XXX: writing ", ll, " bytes: ", eerr)

		// if eerr != nil {
		// 	// prolly a disconnect
		// 	dead = true
		// }
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
func (t *PassthroughTransport) Close() error { return nil }
func (t *PassthroughTransport) ReadAll(buf []byte) (int, error) {
	return t.Read(buf)
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
 */
type CassandraPassthrough struct {
}

func (c *CassandraPassthrough) Login(auth_request *cassandra.AuthenticationRequest) (authnx *cassandra.AuthenticationException, authzx *cassandra.AuthorizationException, err error) {
	k1, _ := auth_request.Credentials.Get("username")
	k2, _ := auth_request.Credentials.Get("password")
	log.Print("CassandraPassthrough:Login ", auth_request.Credentials, " ", k1, " ", k2)
	return nil, nil, nil
}

func (c *CassandraPassthrough) SetKeyspace(keyspace string) (ire *cassandra.InvalidRequestException, err error) {
	log.Print("CassandraPassthrough:SetKeyspace ks=", keyspace)
	return nil, nil
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
