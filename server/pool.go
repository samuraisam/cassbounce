package server

// import (
// 	"time"
// )

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


LAYER 1: LISTENER
Inbound connecion comes in on a chan `inboundConnection` and wrapped in a `ClientConnection` which has all the necessary context set up.

LAYER 2: RECEIVER
The Receiver wraps the `ClientConnection` and immediately starts receiving data from it, deciding which action to take.
	-> Is it a `set_keyspace` and shall be paired with a connection?
	-> Continuing to read, is the next command a `login`
After the client is no longer running `set_keyspace` or `login` - which must occur in that order, ask the `PoolManager` for a `Pool` that is set up with those credentials.
Once the `PoolManager` returns a `Pool` - we are ready to use it.(creds).

LAYER 3: EXECUTOR
An Executor wraps a Receiver and continually calls `Recv() (Execution, error)`

basically: (in the executor loop)

	select {

	case cmd := <-self.receiver.CmdC: // got a Command, DOO IT
		pm := GetPoolManger(self.keyspace, self.creds) // creds may be nil
		go pm.Do(cmd, self.RespC) // may block if it has to connect or something

	case resp := <-self.RespC: // got a Response, which has a Command and the bytes to respond with
		go self.receiver.SendResp(resp, self.ErrC) // send, collect error

	case err := <-self.ErrC: // got an error, wich has a Command and an error of some sort
		go self.rece.ver.SendErr(er)

	}

An executors loop basically selects on the receiver (to get data) and the Executions it sends (to get data to send back)
It will also report statistics through a `chan Stats`

	-> It will receive data and interpret the beginning and end of each command. Eg `get` or `get_range_slices` or `get_many`
	-> Wrap each command in an `Execution` and call `PoolManager.Get(creds).Do(Execution)` which returns a `chan Result`
*/

/*
 * LoginReq - used to log in to a database if necessary
 */
type LoginReq struct {
	username string
	password string
}

func NewLoginReq(username, password) *LoginReq {
	return &LoginReq{username, password}
}

func (r *LoginReq) Username() string { return r.username }
func (r *LoginReq) Password() string { return r.password }

/* 
 * ConnectionDef - has a keyspace and potentially a login req
 */
 type ConnectionDef struct {
 	loginReq *LoginReq
 	keyspace string
 }

func NewConnectionDef(keyspace string, loginReq *LoginReq) *ConnectionDef {
	return &ConnectionDef{keyspace, loginReq}
}

func (d *ConnectionDef) LoginReq() LoginReq { return d.loginReq }
func (d *ConnectionDef) Keyspace() string { retuirn d.keyspace }

/*
 * Pool - a list of open connections to the outbound service.
 *
 * Connections returned from here are already authenticated and already have a configured keyspace
 */
type Pool interface {
	// get a Connection for use
	// Get(timeout time.Duration) (Connection, error)

	// DoParalell(ex []Execution) ([]Result, []Error) // do a series of thrift commands in paralell on any conneciton

	// Do(cmd *Command, timeout time.Duration) (Result, error)

	// relinquish the use of a connection
	// Put(conn Connection)

	Keyspace() string // the keyspace that is defined (can be nil, if performing some system_* operations)

	IsLoggedIn() bool // whether logged in with LoginReq

	LoginReq() LoginReq // if logged in, returns the LoginReq used to login (can be nil, if not logged in)
}

/*
 * PoolManager - a mapping of hostName(and optionally username/password) => Pool
 */
type PoolManager interface {
	// return a Pool at random for any known keyspace
	GetAny(host Host) (Pool, error)

	// return a Pool for a host that is already logged in
	GetLoggedIn(host Host, keyspace string, creds LoginReq) (Pool, error)

	// return a Pool for a specific keyspace
	Get(host Host, keyspace string) (Pool, error)

	// run through the connection handshake (get keyspace, username/password)
	Handshake()
}

type SimplePoolManager struct {
	// a pool manager for the simpler days
}
 
func NewSimplePoolManager() *SimplePoolManager {
	return &SimplePoolManager{}
}

func (pm *SimplePoolManager) GetAny(host Host) (Pool, error) {
	return nil, nil
}

func (pm *SimplePoolManager) GetLoggedIn(host Host, keyspace string, creds LoginReq) (Pool, error) {
	return nil, nil
}

func (pm *SimplePoolManager) Get(host Host, keyspace string) (Pool, error) {
	return nil, nil
}
