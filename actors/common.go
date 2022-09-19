package actors

import (
	"net/http"

	"github.com/nats-io/nats.go"

	"github.com/ez-framework/ez-framework/configkv"
)

type IJetStreamActor interface {
	Run()
	Publish(string, []byte) error
	ServeHTTP(http.ResponseWriter, *http.Request)
	OnBootLoad() error

	kv() nats.KeyValue
}

type GlobalConfig struct {
	NatsAddr         string
	HTTPAddr         string
	NatsConn         *nats.Conn
	JetStreamContext nats.JetStreamContext
	ConfigKV         *configkv.ConfigKV
}
