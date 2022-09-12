package actor

import "github.com/nats-io/nats.go"

type IJetStreamActor interface {
	retrySubscribing(string) *nats.Subscription
	Run()
}
