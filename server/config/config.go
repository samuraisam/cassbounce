package config

import (
	"github.com/rcrowley/go-metrics"
	"time"
)

// App settings - global app configuration
type AppSettings struct {
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
	settings        *AppSettings     // global settings used throughout the app
	metricsRegistry metrics.Registry // global metrics registry
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
	// set up metrics reporting
	currentApp.metricsRegistry = metrics.NewRegistry()

	return currentApp
}

// TODO: this could respond to some environmental change
func (a *App) SetSettings(s *AppSettings) { a.settings = s }
func (a *App) Settings() *AppSettings     { return a.settings }

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
