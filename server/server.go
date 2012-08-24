package server

import (
	"log"
	"net"
	// "time"
	// "github.com/rcrowley/go-metrics"
	"cassbounce/server/config"
)

// Global app object
type App struct {
	hostList     HostList    // host list service - manages a list of up/down outbound servers
	ShutdownChan chan int    // channel used when we want to shut down services
	poolMan      PoolManager // pool manager service - manages a list outbound connection pools
}

func New() *App {
	return &App{}
}

func (a *App) SetHostList(h HostList) { a.hostList = h }
func (a *App) HostList() HostList     { return a.hostList }

func (a *App) poolManager() PoolManager { // lazily load a PoolManager
	if a.poolMan == nil {
		switch config.Get().Settings().PoolManagerType {
		case "simple":
		default:
			a.poolMan = NewSimplePoolManager()
		}
	}
	return a.poolMan
}

// listen forever on the interface a.ListenAddress
func (a *App) Listen() error {
	listener, err := net.Listen("tcp", config.Get().Settings().ListenAddress)
	if err != nil {
		return err
	}
	log.Print("App:Listen now accepting connections on interface: ", config.Get().Settings().ListenAddress)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Print("App:Listen error accepting connection: ", err)
			continue
		}
		// create a receiver for this connection
		var receiver Receiver
		rtype := config.Get().Settings().ReceiverType

		if rtype == "command" {
			receiver = NewCommandReceiver(conn, a.hostList, a.poolManager())
		}

		go receiver.Receive()
	}
	return nil
}
