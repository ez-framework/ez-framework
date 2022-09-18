package actors

import (
	"net/http"

	"github.com/nats-io/nats.go"
)

type IJetStreamActor interface {
	Run()
	Publish(string, []byte) error
	ServeHTTP(http.ResponseWriter, *http.Request)

	kv() nats.KeyValue
}
