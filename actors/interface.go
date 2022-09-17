package actors

import (
	"net/http"

	"github.com/nats-io/nats.go"
)

type IJetStreamActor interface {
	Run()
	Publish(string, []byte) error
	ServeHTTP(w http.ResponseWriter, r *http.Request)

	kv() nats.KeyValue
	retrySubscribing(string) *nats.Subscription
}
