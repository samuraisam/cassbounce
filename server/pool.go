package server

import (
	"time"
)

/*
The pool - the design of this is as follows

We have a bunch of clients on our local server, for each request they serve, a 
connection will likely be opened up to cassbounce. the connection will be held
open until the end of the request, at which point the connection will be severed.

	1. inbound connection comes in from the listener
		1a. determine if it is a set_keyspace - if so, keep track of the set_keyspace - also login, the
		sequence of operations is set_keyspace then login(LoginReq)
		1b. if not set_keyspace - make sure it's one of the operations that does not require a keyspace
		(some of the system_* functions)
		1c. otherwise refuse the connection (don't know what to do with it)
	2. it asks the Pool for an outbound connection: Get(timeout)
		2a. if the pool doesn't have an available connection, a new one will be opened
		2b. ask HostList to get a host (it will round-robin amongst available hosts)
		2c. ask PoolManager for a pool for the host
			2c1. if PoolManager does not have a Pool for the host, one is created, and in the background warmed
			2c2. if creating one, return a new connection as early as possible
	3. inbound connection has use of the connection as long as it wants
	4. inbound connection will relinquish its use of the outbound connection
		4a. if the pool is already full, the connection is severed - in that vein, the pool size should be as large
		as is needed to satisfy any inbound connections including spikes.
*/

/*
 * LoginReq - used to log in to a database if necessary
 */
type LoginReq interface {
	Username() string
	Password() string
}

/*
 * Pool - a list of open connections to the outbound service.
 *
 * Connections returned from here are already authenticated and already have a configured keyspace
 */
type Pool interface {
	// get a Connection for use
	Get(timeout time.Duration) (Connection, error)

	// DoParalell(ex []Execution) ([]Result, []Error) // do a series of thrift commands in paralell on any conneciton

	// relinquish the use of a connection
	Put(conn Connection)

	Keyspace() string // the keyspace that is defined (can be nil, if performing some system_* operations)

	IsLoggedIn() bool // whether logged in with LoginReq

	LoginReq() LoginReq // if logged in, returns the LoginReq used to login (can be nil, if not logged in)
}

/*
 * PoolManager - a mapping of hostName=>Pool
 */
type PoolManager interface {
	// return a Pool at random for any known keyspace
	GetAny(host Host) (Pool, error)

	// return a Pool for a host that is already logged in
	GetLoggedIn(host Host, keyspace string, creds LoginReq) (Pool, error)

	// return a Pool for a specific keyspace
	Get(host Host, keyspace string) (Pool, error)
}
