package actors

import "github.com/nats-io/nats.go"

type IJetStreamActor interface {
	Run()
	Publish(string, []byte) error

	setupConfigKVStore() error
	retrySubscribing(string) *nats.Subscription
}
