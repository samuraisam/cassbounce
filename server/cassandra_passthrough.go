package server

import (
	"github.com/pomack/thrift4go/lib/go/src/thrift"
	"github.com/carloscm/gossie/src/cassandra"
)

/*
 * CassandraPassthrough
 *
 * Implements all of `ICassandra` so we may hijack some communications with the backend servers
 *
 * Nothing is *actually* implemented, all methods return zero-d values. It's meant to be "subtyped"
 */
type CassandraPassthrough struct {
}

func (c *CassandraPassthrough) Login(auth_request *cassandra.AuthenticationRequest) (authnx *cassandra.AuthenticationException, authzx *cassandra.AuthorizationException, err error) {
	return nil, nil, nil
}

func (c *CassandraPassthrough) SetKeyspace(keyspace string) (ire *cassandra.InvalidRequestException, err error) {
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
