package actors

import (
	"net/http"

	"github.com/nats-io/nats.go"

	"github.com/ez-framework/ez-framework/configkv"
)

// IActor is the interface to conform to for all actors
type IActor interface {
	RunOnConfigUpdate()
	Publish(string, []byte) error
	ServeHTTP(http.ResponseWriter, *http.Request)
	OnBootLoadConfig() error

	kv() nats.KeyValue
}

// ActorConfig is the config that all actors need
type ActorConfig struct {
	// HTTPAddr is the address to bind the HTTP server
	HTTPAddr string

	// NatsAddr is the address to connect to
	NatsAddr string

	// NatsConn is the connection to a NATS cluster
	NatsConn *nats.Conn

	JetStreamContext nats.JetStreamContext

	StreamConfig *nats.StreamConfig

	// ConfigKV is the KV store available for all actors.
	ConfigKV *configkv.ConfigKV
}
