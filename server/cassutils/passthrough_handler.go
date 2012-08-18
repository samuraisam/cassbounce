package cassutils

import (
	"github.com/carloscm/gossie/src/cassandra"
	"github.com/pomack/thrift4go/lib/go/src/thrift"
)

/*
PassthroughHandler

Implements all of `ICassandra` so we may hijack some communications with the backend servers

Nothing is *actually* implemented, all methods return zero-d values. It's meant to be "subtyped"
*/
type PassthroughHandler struct {
}

func (c *PassthroughHandler) Login(auth_request *cassandra.AuthenticationRequest) (authnx *cassandra.AuthenticationException, authzx *cassandra.AuthorizationException, err error) {
	return nil, nil, nil
}

func (c *PassthroughHandler) SetKeyspace(keyspace string) (ire *cassandra.InvalidRequestException, err error) {
	return nil, nil
}

func (c *PassthroughHandler) Get(key []byte, column_path *cassandra.ColumnPath, consistency_level cassandra.ConsistencyLevel) (retval448 *cassandra.ColumnOrSuperColumn, ire *cassandra.InvalidRequestException, nfe *cassandra.NotFoundException, ue *cassandra.UnavailableException, te *cassandra.TimedOutException, err error) {
	return nil, nil, nil, nil, nil, nil
}

func (c *PassthroughHandler) GetSlice(key []byte, column_parent *cassandra.ColumnParent, predicate *cassandra.SlicePredicate, consistency_level cassandra.ConsistencyLevel) (retval449 thrift.TList, ire *cassandra.InvalidRequestException, ue *cassandra.UnavailableException, te *cassandra.TimedOutException, err error) {
	return nil, nil, nil, nil, nil
}

func (c *PassthroughHandler) GetCount(key []byte, column_parent *cassandra.ColumnParent, predicate *cassandra.SlicePredicate, consistency_level cassandra.ConsistencyLevel) (retval450 int32, ire *cassandra.InvalidRequestException, ue *cassandra.UnavailableException, te *cassandra.TimedOutException, err error) {
	return 0, nil, nil, nil, nil
}

func (c *PassthroughHandler) MultigetSlice(keys thrift.TList, column_parent *cassandra.ColumnParent, predicate *cassandra.SlicePredicate, consistency_level cassandra.ConsistencyLevel) (retval451 thrift.TMap, ire *cassandra.InvalidRequestException, ue *cassandra.UnavailableException, te *cassandra.TimedOutException, err error) {
	return nil, nil, nil, nil, nil
}

func (c *PassthroughHandler) MultigetCount(keys thrift.TList, column_parent *cassandra.ColumnParent, predicate *cassandra.SlicePredicate, consistency_level cassandra.ConsistencyLevel) (retval452 thrift.TMap, ire *cassandra.InvalidRequestException, ue *cassandra.UnavailableException, te *cassandra.TimedOutException, err error) {
	return nil, nil, nil, nil, nil
}

func (c *PassthroughHandler) GetRangeSlices(column_parent *cassandra.ColumnParent, predicate *cassandra.SlicePredicate, range_a1 *cassandra.KeyRange, consistency_level cassandra.ConsistencyLevel) (retval453 thrift.TList, ire *cassandra.InvalidRequestException, ue *cassandra.UnavailableException, te *cassandra.TimedOutException, err error) {
	return nil, nil, nil, nil, nil
}

func (c *PassthroughHandler) GetIndexedSlices(column_parent *cassandra.ColumnParent, index_clause *cassandra.IndexClause, column_predicate *cassandra.SlicePredicate, consistency_level cassandra.ConsistencyLevel) (retval454 thrift.TList, ire *cassandra.InvalidRequestException, ue *cassandra.UnavailableException, te *cassandra.TimedOutException, err error) {
	return nil, nil, nil, nil, nil
}

func (c *PassthroughHandler) Insert(key []byte, column_parent *cassandra.ColumnParent, column *cassandra.Column, consistency_level cassandra.ConsistencyLevel) (ire *cassandra.InvalidRequestException, ue *cassandra.UnavailableException, te *cassandra.TimedOutException, err error) {
	return nil, nil, nil, nil
}

func (c *PassthroughHandler) Add(key []byte, column_parent *cassandra.ColumnParent, column *cassandra.CounterColumn, consistency_level cassandra.ConsistencyLevel) (ire *cassandra.InvalidRequestException, ue *cassandra.UnavailableException, te *cassandra.TimedOutException, err error) {
	return nil, nil, nil, nil
}

func (c *PassthroughHandler) Remove(key []byte, column_path *cassandra.ColumnPath, timestamp int64, consistency_level cassandra.ConsistencyLevel) (ire *cassandra.InvalidRequestException, ue *cassandra.UnavailableException, te *cassandra.TimedOutException, err error) {
	return nil, nil, nil, nil
}

func (c *PassthroughHandler) RemoveCounter(key []byte, path *cassandra.ColumnPath, consistency_level cassandra.ConsistencyLevel) (ire *cassandra.InvalidRequestException, ue *cassandra.UnavailableException, te *cassandra.TimedOutException, err error) {
	return nil, nil, nil, nil
}

func (c *PassthroughHandler) BatchMutate(mutation_map thrift.TMap, consistency_level cassandra.ConsistencyLevel) (ire *cassandra.InvalidRequestException, ue *cassandra.UnavailableException, te *cassandra.TimedOutException, err error) {
	return nil, nil, nil, nil
}

func (c *PassthroughHandler) Truncate(cfname string) (ire *cassandra.InvalidRequestException, ue *cassandra.UnavailableException, err error) {
	return nil, nil, nil
}

func (c *PassthroughHandler) DescribeSchemaVersions() (retval461 thrift.TMap, ire *cassandra.InvalidRequestException, err error) {
	return nil, nil, nil
}

func (c *PassthroughHandler) DescribeKeyspaces() (retval462 thrift.TList, ire *cassandra.InvalidRequestException, err error) {
	return nil, nil, nil
}

func (c *PassthroughHandler) DescribeClusterName() (retval463 string, err error) { return "", nil }

func (c *PassthroughHandler) DescribeVersion() (retval464 string, err error) { return "", nil }

func (c *PassthroughHandler) DescribeRing(keyspace string) (retval465 thrift.TList, ire *cassandra.InvalidRequestException, err error) {
	return nil, nil, nil
}

func (c *PassthroughHandler) DescribePartitioner() (retval466 string, err error) { return "", nil }

func (c *PassthroughHandler) DescribeSnitch() (retval467 string, err error) { return "", nil }

func (c *PassthroughHandler) DescribeKeyspace(keyspace string) (retval468 *cassandra.KsDef, nfe *cassandra.NotFoundException, ire *cassandra.InvalidRequestException, err error) {
	return nil, nil, nil, nil
}

func (c *PassthroughHandler) DescribeSplits(cfName string, start_token string, end_token string, keys_per_split int32) (retval469 thrift.TList, ire *cassandra.InvalidRequestException, err error) {
	return nil, nil, nil
}

func (c *PassthroughHandler) SystemAddColumnFamily(cf_def *cassandra.CfDef) (retval470 string, ire *cassandra.InvalidRequestException, sde *cassandra.SchemaDisagreementException, err error) {
	return "", nil, nil, nil
}

func (c *PassthroughHandler) SystemDropColumnFamily(column_family string) (retval471 string, ire *cassandra.InvalidRequestException, sde *cassandra.SchemaDisagreementException, err error) {
	return "", nil, nil, nil
}

func (c *PassthroughHandler) SystemAddKeyspace(ks_def *cassandra.KsDef) (retval472 string, ire *cassandra.InvalidRequestException, sde *cassandra.SchemaDisagreementException, err error) {
	return "", nil, nil, nil
}

func (c *PassthroughHandler) SystemDropKeyspace(keyspace string) (retval473 string, ire *cassandra.InvalidRequestException, sde *cassandra.SchemaDisagreementException, err error) {
	return "", nil, nil, nil
}

func (c *PassthroughHandler) SystemUpdateKeyspace(ks_def *cassandra.KsDef) (retval474 string, ire *cassandra.InvalidRequestException, sde *cassandra.SchemaDisagreementException, err error) {
	return "", nil, nil, nil
}

func (c *PassthroughHandler) SystemUpdateColumnFamily(cf_def *cassandra.CfDef) (retval475 string, ire *cassandra.InvalidRequestException, sde *cassandra.SchemaDisagreementException, err error) {
	return "", nil, nil, nil
}

func (c *PassthroughHandler) ExecuteCqlQuery(query []byte, compression cassandra.Compression) (retval476 *cassandra.CqlResult, ire *cassandra.InvalidRequestException, ue *cassandra.UnavailableException, te *cassandra.TimedOutException, sde *cassandra.SchemaDisagreementException, err error) {
	return nil, nil, nil, nil, nil, nil
}
