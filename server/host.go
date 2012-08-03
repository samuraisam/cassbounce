package server

import (
	"time"
	"log"
	"fmt"
)

/*
 * CassandraHost ------------------------------------------------------------------------------------------
 * 
 * CassandraHost represents a single host that is reachable via thrift.
 */
type CassandraHost struct {
	Host string
	Port int
}

func (h CassandraHost) String() string {
	return fmt.Sprintf("%s:%d", h.Host, h.Port)
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
type CassandraHostList struct {
	Up map[string]CassandraHost
	Down map[string]CassandraHost
	shutdown chan int 
}

func NewCassandraHostList(initialHostList []CassandraHost, useAutodiscovery bool, shutdown chan int) *CassandraHostList {
	up := make(map[string]CassandraHost)
	dwn := make(map[string]CassandraHost)
	ret := &CassandraHostList{up, dwn, shutdown}

	for i := range initialHostList {
		ret.Down[initialHostList[i].String()] = initialHostList[i]
	}

	// TODO: configurable whether or not to poll, poll frequency
	ret.Poll(true, true, time.Duration(5) * time.Second) // comb the up list to include only up nodes

	if useAutodiscovery {
		ret.NodeAutoDiscovery(time.Duration(10) * time.Second) // TODO: make configurable
	}
	return ret
}

func (l *CassandraHostList) Poll(doImmediate bool, continueUntilQuit bool, frequency time.Duration) {
	// go through the Up and Down lists and move hosts into the right category
	pollFinished := make(chan int)
	readyForNextPoll := false

	if doImmediate {
		l.doPoll(pollFinished)
		<-pollFinished
	}

	if !continueUntilQuit {
		return // just exit if we have no directions to continue until quit
	}

	for {
		select {
		case <-l.shutdown:
			log.Print("CassandraHostList:Poll sent shutdown")
		case <-pollFinished:
			readyForNextPoll = true // a poll finished
		case <-time.After(frequency):
			if readyForNextPoll {
				// the previous poll completed after some waiting, now do the next poll
				l.doPoll(pollFinished)
			}
		}
	}
}

func (l *CassandraHostList) doPoll(pollFinished chan int) {
	log.Print("CassandraHostList:Poll:doPoll starting poll")
	nTested := 0
	didComplete := make(chan int)
	newDown := make(map[string]CassandraHost)
	// test all the connetions in Up
	for s, host := range l.Up {
		nTested += 1
		go func() {
			defer func() { didComplete <- 1 }() // notify of completion when done
			// test asynchronously whether or not this is available
			ok := host.Test(time.Duration(1)*time.Second) // TODO: configurable poll instance timeout
			if !ok {
				newDown[s] = host // it was previously up, but no more :( - add it to Down
			}
		}()
	}
	// test all the connections in Down
	newUp := make(map[string]CassandraHost)
	for s, host := range l.Down {
		nTested += 1
		go func() {
			defer func() { didComplete <- 1 }() // notify of completion when done
			ok := host.Test(time.Duration(1)*time.Second) // TODO: configurable poll instance timeout
			if ok {
				newUp[s] = host // it was previously down, but no more!, add it to Up
			}
		}()
	}
	// TODO: configure poll list timeout
	timeouter := time.After(time.Duration(10)*time.Second) // wait N seconds for all polls to finish
	nComplete := 0
	// wait for all the tests to complete, or for timeouter to time out, and send pollFinished
	for {
		select {
		case <-timeouter: 
			// bail if timed out
			log.Print("CassandraHostList:Poll:doPoll polling timed out - finished ", nTested, " of ", nComplete, " servers")
			pollFinished <- 1 // notify finished (albiet timed out)
			return
		case <-didComplete:
			nComplete += 1
			if nComplete == nTested {
				log.Print("CassandraHostList:Poll:doPoll finished polling ", nTested, "servers")
				l.updateLists(newUp, newDown) // update our alive/dead lists
				pollFinished <- 1 // notify finished
				return
			}
		}
	}
}

func (l *CassandraHostList) updateLists(newUp map[string]CassandraHost, newDown map[string]CassandraHost) {
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
}

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
			
		}
	}
}