package actors

import (
	"net/http"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"

	"github.com/ez-framework/ez-framework/configkv"
)

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

// IActor is the interface to conform to for all actors
type IActor interface {
	GetStreamName() string
	RunSubscriberAsync()
	RunSubscriberSync(msg *nats.Msg)
	Publish(string, []byte) error
	ServeHTTP(http.ResponseWriter, *http.Request)
	OnBootLoadConfig() error
	SetPOSTSubscriber(func(msg *nats.Msg))
	SetPUTSubscriber(func(msg *nats.Msg))
	SetDELETESubscriber(func(msg *nats.Msg))

	jc() nats.JetStreamContext
	kv() nats.KeyValue
}

// Actor is the base struct for all actors.
// It provides common helper functions and conforms to IActor.
type Actor struct {
	ConfigKV *configkv.ConfigKV

	actorConfig      ActorConfig
	streamName       string
	postSubscriber   func(msg *nats.Msg)
	putSubscriber    func(msg *nats.Msg)
	deleteSubscriber func(msg *nats.Msg)
	subscription     *nats.Subscription
	infoLogger       *zerolog.Event
	errorLogger      *zerolog.Event
	debugLogger      *zerolog.Event
}

// setupStream creates a dedicated stream for this actor
func (actor *Actor) setupStream() error {
	if actor.actorConfig.StreamConfig == nil {
		actor.actorConfig.StreamConfig = &nats.StreamConfig{}
	}

	actor.actorConfig.StreamConfig.Name = actor.streamName
	actor.actorConfig.StreamConfig.Subjects = append(actor.actorConfig.StreamConfig.Subjects, actor.streamName+".>")

	_, err := actor.jc().AddStream(actor.actorConfig.StreamConfig)
	if err != nil {
		if err.Error() == "nats: stream name already in use" {
			_, err = actor.jc().UpdateStream(actor.actorConfig.StreamConfig)
		}

		if err != nil {
			actor.errorLogger.Caller().Err(err).
				Str("stream.name", actor.streamName).
				Msg("failed to create or get a stream")

			return err
		}
	}

	return nil
}

// subscribeSubjects is usually the streamName followed by any matching characters.
// Example: stream-name.>. The greater than symbol means match more than one subpaths.
// NATS subpaths are delimited with dots.
func (actor *Actor) subscribeSubjects() string {
	return actor.streamName + ".>"
}

// kv gets the underlying KV store
func (actor *Actor) jc() nats.JetStreamContext {
	return actor.actorConfig.JetStreamContext
}

// kv gets the underlying KV store
func (actor *Actor) kv() nats.KeyValue {
	return actor.ConfigKV.KV
}

// keyWithCommand appends the command at the end of the key.
// The nats key looks like this: stream-name.optional-key.command:POST|PUT|DELETE.
func (actor *Actor) keyWithCommand(key, command string) string {
	return key + ".command:" + command
}

// keyWithoutCommand strips the command which is appended at the end.
// The nats key looks like this: stream-name.optional-key.command:POST|PUT|DELETE.
func (actor *Actor) keyWithoutCommand(key string) string {
	keyEndIndex := strings.Index(key, ".command:")
	return key[0:keyEndIndex]
}

// keyHasCommand checks if the nats key has a command.
// The nats key looks like this: stream-name.optional-key.command:POST|PUT|DELETE.
func (actor *Actor) keyHasCommand(key, command string) bool {
	return strings.HasSuffix(key, ".command:"+command)
}

// unsubscribeFromOnConfigUpdate
func (actor *Actor) unsubscribeFromOnConfigUpdate() error {
	if actor.subscription != nil {
		return actor.subscription.Unsubscribe()
	}

	return nil
}

func (actor *Actor) GetStreamName() string {
	return actor.streamName
}

// SetPOSTSubscriber
func (actor *Actor) SetPOSTSubscriber(handler func(msg *nats.Msg)) {
	actor.postSubscriber = handler
}

// SetPUTSubscriber
func (actor *Actor) SetPUTSubscriber(handler func(msg *nats.Msg)) {
	actor.putSubscriber = handler
}

// SetDELETESubscriber
func (actor *Actor) SetDELETESubscriber(handler func(msg *nats.Msg)) {
	actor.deleteSubscriber = handler
}

// Publish data into JetStream with a nats key.
// The nats key looks like this: stream-name.optional-key.command:POST|PUT|DELETE.
func (actor *Actor) Publish(key string, data []byte) error {
	_, err := actor.jc().Publish(key, data)
	if err != nil {
		actor.errorLogger.Caller().Err(err).
			Str("publish.key", key).
			Msg("failed to publish to JetStream")
	}

	actor.infoLogger.
		Str("publish.key", key).
		Msg("published to JetStream")

	return err
}

// RunSubscriberSync executes the subscriber handler immediately
func (actor *Actor) RunSubscriberSync(msg *nats.Msg) {
	if actor.keyHasCommand(msg.Subject, "POST") {
		if actor.postSubscriber != nil {
			actor.postSubscriber(msg)
		}
	} else if actor.keyHasCommand(msg.Subject, "PUT") {
		if actor.putSubscriber != nil {
			actor.putSubscriber(msg)
		}
	} else if actor.keyHasCommand(msg.Subject, "DELETE") {
		if actor.deleteSubscriber != nil {
			actor.deleteSubscriber(msg)
		}
	}
}

// RunSubscriberAsync listens to config changes and execute hooks
func (actor *Actor) RunSubscriberAsync() {
	actor.infoLogger.
		Str("subjects", actor.subscribeSubjects()).
		Msg("subscribing to nats subjects")

	var err error
	var sub *nats.Subscription

	switch actor.actorConfig.StreamConfig.Retention {
	case nats.WorkQueuePolicy:
		sub, err = actor.jc().QueueSubscribe(actor.subscribeSubjects(), "workers", func(msg *nats.Msg) {
			actor.RunSubscriberSync(msg)
		})
	default:
		sub, err = actor.jc().Subscribe(actor.subscribeSubjects(), func(msg *nats.Msg) {
			actor.RunSubscriberSync(msg)
		})
	}

	if err == nil {
		actor.subscription = sub

	} else {
		actor.errorLogger.Err(err).
			Err(err).
			Str("subjects", actor.subscribeSubjects()).
			Msg("failed to subscribe to subjects")
	}
}

// OnBootLoadConfig
func (actor *Actor) OnBootLoadConfig() error {
	actor.infoLogger.Msg("OnBootLoadConfig() is not implemented")
	return nil
}

// ServeHTTP
func (actor *Actor) ServeHTTP(http.ResponseWriter, *http.Request) {
	actor.infoLogger.Msg("ServeHTTP() is not implemented")
}
