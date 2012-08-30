package server

import (
	"cassbounce/server/config"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"
)

/*
 * Base Types -------------------------------------------------------------------------------------------
 */

type Host interface {
	Host() string
	Port() int
	String() string
	Test(timeout time.Duration) bool
}

type HostList interface {
	Get() (Host, error)
}

/*
 * CassandraHost ------------------------------------------------------------------------------------------
 * 
 * CassandraHost represents a single host that is reachable via thrift.
 */

type CassandraHost struct {
	host string
	port int
}

func NewCassandraHost(addr string, port int) CassandraHost {
	return CassandraHost{addr, port}
}

func (h CassandraHost) Host() string { return h.host }
func (h CassandraHost) Port() int    { return h.port }

func (h CassandraHost) String() string {
	return fmt.Sprintf("%s:%d", h.Host(), h.Port())
}

func (h CassandraHost) Test(timeout time.Duration) bool {
	conn, err := Dial(h.String(), "", timeout)
	if err != nil {
		log.Print("CassandraHost:Test host dead: ", h.String())
		return false
	}
	return conn != nil
}

/*
 * CassandraHostList ---------------------------------------------------------------------------------------
 *
 * A neat little utility which, given a list of initial servers, will periodically test the health of the
 * servers, maintaining a list of good and bad servers. It can also go out and grab more servers by using
 * describe_ring on one of the nodes.
 */
var (
	NoHostsAvailableError = errors.New("No hosts are currently available.")
)

type CassandraHostList struct {
	Up             map[string]CassandraHost // list of up hosts
	Down           map[string]CassandraHost // list of down'd servers
	available      []string                 // list of keys in Up
	shutdown       chan int                 // global shutdown channel used to stop async services
	mtx            sync.Mutex               // used to synchronize mutations to Up and Down
	curIndex       int                      // for round-robin balancing on Up and Down
	alreadyPolling bool                     // used to keep track whether or not StartPolling was called
}

func NewCassandraHostList(initialHostList []CassandraHost, useAutodiscovery bool, shutdown chan int) *CassandraHostList {
	up := make(map[string]CassandraHost)
	dwn := make(map[string]CassandraHost)
	var mtx sync.Mutex
	avail := make([]string, 0)
	ret := &CassandraHostList{up, dwn, avail, shutdown, mtx, 0, false}

	for _, val := range initialHostList {
		ret.Down[val.String()] = val
	}

	// TODO: configurable whether or not to poll, poll frequency
	pollForever := config.Get().Settings().PollServersForever
	ret.StartPollingServers(true, pollForever, time.Duration(5)*time.Second) // comb the up list to include only up nodes

	if useAutodiscovery {
		go ret.NodeAutoDiscovery(time.Duration(10) * time.Second) // TODO: make configurable
	}
	return ret
}

// for easy stringification
func (l *CassandraHostList) String() string {
	tmpl := "<CassandraHostList ServersUp: [%s] ServersDown: [%s]>"
	up := make([]string, len(l.Up))
	down := make([]string, len(l.Down))
	i := 0
	for u, _ := range l.Up {
		up[i] = fmt.Sprintf("%s", u)
		i++
	}
	i = 0
	for d, _ := range l.Down {
		down[i] = fmt.Sprintf("%s", d)
		i++
	}
	return fmt.Sprintf(tmpl, strings.Join(up, ", "), strings.Join(down, ", "))
}

// to satisfy HostList.Get()
// round-robin balances on the list of available hosts 
func (l *CassandraHostList) Get() (host Host, err error) {
	// this method is synchronized so we may increment curIndex and rely on the length of l.Up
	l.mtx.Lock()
	defer func() { l.mtx.Unlock() }()

	if len(l.available) == 0 {
		return nil, NoHostsAvailableError
	}

	if len(l.available) >= l.curIndex { // we may have looped around, or l.Up may have changed size
		l.curIndex = 0
	}

	next := l.Up[l.available[l.curIndex]]
	l.curIndex += 1

	return next, nil
}

func (l *CassandraHostList) StartPollingServers(doImmediate bool, continueUntilQuit bool, frequency time.Duration) {
	// no need to have multiple polling services
	if l.alreadyPolling {
		return
	}
	// go through the Up and Down lists and move hosts into the right category
	pollFinished := make(chan int, 1)
	readyForNextPoll := true

	if doImmediate {
		go l.doPoll(pollFinished)
		<-pollFinished // block for an immedaite poll
	}

	if !continueUntilQuit {
		return // just exit if we have no directions to continue until quit
	}

	l.alreadyPolling = true

	go func() {
		// wait `frequency` to start health checks
		<-time.After(frequency)
		log.Print("CassandraHostList:StartPollingServers beginning server health checks")
		for {
			timeouter := time.After(frequency)
			select {
			case <-l.shutdown:
				log.Print("CassandraHostList:StartPollingServers sent shutdown")
				return
			case <-pollFinished:
				readyForNextPoll = true
			case <-timeouter:
				if readyForNextPoll {
					readyForNextPoll = false
					go l.doPoll(pollFinished)
				}
			}
		}
	}()
}

func doTest(host CassandraHost, didComplete chan int, wasUp bool, newUp map[string]CassandraHost, newDown map[string]CassandraHost) {
	defer func() { didComplete <- 1 }()             // notify of complete when done
	ok := host.Test(time.Duration(1) * time.Second) // test if the host is up9
	if wasUp && !ok {
		newDown[host.String()] = host // it was previously up, but is no more :( - add it to down
	} else if !wasUp && ok {
		newUp[host.String()] = host // it was previously down, but is now up! - add it to up
	}
}

func (l *CassandraHostList) doPoll(pollFinished chan int) {
	log.Print("CassandraHostList:Poll:doPoll starting poll")
	nTested := 0
	didComplete := make(chan int)
	newDown := make(map[string]CassandraHost)
	newUp := make(map[string]CassandraHost)
	// test all the connetions in Up
	for _, host := range l.Up {
		nTested += 1
		go doTest(host, didComplete, true, newUp, newDown)
	}
	// test all the connections in Down
	for _, host := range l.Down {
		nTested += 1
		go doTest(host, didComplete, false, newUp, newDown)
	}
	// TODO: configure poll list timeout
	timeouter := time.After(time.Duration(10) * time.Second) // wait N seconds for all polls to finish
	nComplete := 0
	// wait for all the tests to complete, or for timeouter to time out, and send pollFinished
	for {
		select {
		case <-timeouter:
			// bail if timed out
			log.Print("CassandraHostList:Poll:doPoll polling timed out - finished ", nTested, " of ", nComplete, " servers")
			go l.updateLists(newUp, newDown)
			pollFinished <- 1 // notify finished (albiet timed out)
			return
		case <-didComplete:
			nComplete += 1
			if nComplete == nTested {
				log.Print("CassandraHostList:Poll:doPoll finished polling ", nTested, " servers")
				go l.updateLists(newUp, newDown) // update our alive/dead lists
				pollFinished <- 1                // notify finished
				return
			}
		}
	}
}

func (l *CassandraHostList) updateLists(newUp map[string]CassandraHost, newDown map[string]CassandraHost) {
	// synchronize this function so we may rely on the length and state of l.Up and l.Down
	l.mtx.Lock()
	defer func() { l.mtx.Unlock() }()
	// take care of newly alive hosts
	for s, host := range newUp {
		// if it was in Down, remove it
		delete(l.Down, s)
		// and add it to Up
		l.Up[s] = host
		log.Print("CassandraHostList:Poll:doPoll marking ", s, " as up!")
	}
	// take care of newly dead hosts
	for s, host := range newDown {
		// if it was in Up, remove it
		delete(l.Up, s)
		// and add it to Down
		l.Down[s] = host
		log.Print("CassandraHostList:Poll:doPoll marking ", s, " as down :(")
	}
	// update our list of available hosts
	avail := make([]string, len(l.Up))
	i := 0 // TODO: figure out if this is a stable enough sort to use as a the list
	for k := range l.Up {
		avail[i] = k
		i++
	}
	sort.Strings(avail)
	l.available = avail
}

// TODO: implement
func (l *CassandraHostList) NodeAutoDiscovery(frequency time.Duration) {
	log.Print("CassandraHostList:NodeAutoDiscovery starting up")
	for {
		select {
		case <-l.shutdown:
			log.Print("CassandraHostList:NodeAutoDiscovery sent shutdown")
			return
		case <-time.After(frequency):
			log.Print("CassandraHostList:NodeAutoDiscovery waking up ")
			// get a new connection to cassandra
			host, err := l.Get()
			if err != nil {
				log.Print("CassandraHostList:NodeAutoDiscovery error getting initial host for discovery: ", err)
				continue
			}
			timeout := time.Duration(100) * time.Millisecond // TODO: configure timeout
			conn, err := Dial(host.String(), "", timeout)
			if err != nil {
				log.Print("CassandraHostList:NodeAutoDiscovery error gettring connection to initial host for discovery: ", err)
				continue
			}
			ring, ire, err := conn.client.DescribeRing("cassbounce")
			log.Print("ring: ", ring, "ire: ", ire, "err: ", err)
		}
	}
}
