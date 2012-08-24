package config

import (
	// "log"
	// "net"
	"time"
	"github.com/rcrowley/go-metrics"
)

// App settings - global app configuration
type AppSettings struct {
	// InitialServerList            []Host        // list of hosts that was passed into the command line
	NodeAutodiscovery            bool          // whether or not to set the HostList to autodiscover
	ListenAddress                string        // what interface to listen for new clients on
	ReceiverType                 string        // what receiver to use (for now, must only be "command")
	PoolManagerType              string        // what pool manager to use (for now, must only be "simple")
	PollServersForever           bool          // whether or not to constantly perform health checks on remote hosts
	PoolRecycleDuration          time.Duration // how frequently to recycle connections to upstream hosts
	PoolRecycleJitter            int           // jitter applied to recycle algorithm so all connections aren't closed at once
	PoolSize                     int           // maximum number of connections maintained to a given upstream server
	PoolConnectionAcquireTimeout time.Duration // wait this long to acquire a connection from a pool
	PoolConnectionAcquireRetries int           // number of times to endure connection aquisition failure before returning error to client
}

// Get a new app settings configured with all the defaults
func NewAppSettings() *AppSettings {
	return &AppSettings{
		ReceiverType:                 "command",
		PoolManagerType:              "simple",
		NodeAutodiscovery:            false,
		ListenAddress:                "0.0.0.0:9160",
		PollServersForever:           false,
		PoolRecycleDuration:          time.Duration(60) * time.Second,
		PoolRecycleJitter:            10,
		PoolSize:                     10,
		PoolConnectionAcquireTimeout: time.Duration(100) * time.Millisecond,
		PoolConnectionAcquireRetries: 2,
	}
}

// Global app object
type App struct {
	// hostList     HostList     // host list service - manages a list of up/down outbound servers
	settings     *AppSettings // global settings used throughout the app
	ShutdownChan chan int     // channel used when we want to shut down services
	// poolMan      PoolManager  // pool manager service - manages a list outbound connection pools
	metricsRegistry metrics.Registry
}

// the global app object
var currentApp *App

func Get() *App {
	// app is a singleton, meaning we can only have one instance
	if currentApp != nil {
		// app was already created, return that one
		return currentApp
	}
	// create a new one since it didn't exist already
	currentApp = &App{}
	currentApp.metricsRegistry = metrics.NewRegistry()
	return currentApp
}

// TODO: this could respond to some environmental change
func (a *App) SetSettings(s *AppSettings) { a.settings = s }
func (a *App) Settings() *AppSettings     { return a.settings }

// func (a *App) SetHostList(h HostList) { a.hostList = h }
// func (a *App) HostList() HostList     { return a.hostList }

// func (a *App) poolManager() PoolManager { // lazily load a PoolManager
// 	if a.poolMan == nil {
// 		switch a.settings.PoolManagerType {
// 		case "simple":
// 		default:
// 			a.poolMan = NewSimplePoolManager()
// 		}
// 	}
// 	return a.poolMan
// }

// listen forever on the interface a.ListenAddress
// func (a *App) Listen() error {
// 	listener, err := net.Listen("tcp", a.Settings().ListenAddress)
// 	if err != nil {
// 		return err
// 	}
// 	log.Print("App:Listen now accepting connections on interface: ", a.settings.ListenAddress)
// 	for {
// 		conn, err := listener.Accept()
// 		if err != nil {
// 			log.Print("App:Listen error accepting connection: ", err)
// 			continue
// 		}
// 		// create a receiver for this connection
// 		var receiver Receiver
// 		rtype := a.settings.ReceiverType

// 		if rtype == "command" {
// 			receiver = NewCommandReceiver(conn, a.hostList, a.poolManager())
// 		}

// 		go receiver.Receive()
// 	}
// 	return nil
// }

func (a *App) Timer(n string) metrics.Timer {
	var t metrics.Timer = nil
	if ti := a.metricsRegistry.Get(n); ti == nil {
		t = metrics.NewTimer()
		a.metricsRegistry.Register(n, t)
	} else {
		t = ti.(metrics.Timer)
	}
	return t
}
