package actors

import (
	"context"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"

	"github.com/ez-framework/ez-framework/configkv"
	"github.com/ez-framework/ez-framework/http_helpers"
)

// ActorConfig is the config that all actors need
type ActorConfig struct {
	// Workers is the number of workers for this actor
	Workers int

	// HTTPAddr is the address to bind the HTTP server
	HTTPAddr string

	// NatsAddr is the address to connect to
	NatsAddr string

	// NatsConn is the connection to a NATS cluster
	NatsConn *nats.Conn

	JetStreamContext nats.JetStreamContext

	StreamConfig *nats.StreamConfig

	StreamChanBuffer int

	// ConfigKV is the KV store available for all actors.
	ConfigKV *configkv.ConfigKV
}

// IActor is the interface to conform to for all actors
type IActor interface {
	GetStreamName() string
	RunConfigListener(context.Context)
	PublishConfig(string, []byte) error
	ServeHTTP(http.ResponseWriter, *http.Request)
	OnBootLoadConfig() error

	SetOnConfigUpdate(func(context.Context, *nats.Msg))
	SetOnConfigDelete(func(context.Context, *nats.Msg))
	SetDownstreams(...string)

	setupStream() error
	jc() nats.JetStreamContext
	kv() nats.KeyValue
	unsubscribeAll() error
}

// Actor is the base struct for all actors.
// It provides common helper functions and conforms to IActor.
type Actor struct {
	ConfigKV *configkv.ConfigKV

	streamName        string
	actorConfig       ActorConfig
	wg                sync.WaitGroup
	subscribers       map[string]func(context.Context, *nats.Msg)
	downstreams       []string
	onDoneSubscribing func() error
	subscriptionChan  chan *nats.Msg
	subscriptions     []*nats.Subscription
	infoLogger        *zerolog.Event
	errorLogger       *zerolog.Event
	debugLogger       *zerolog.Event
}

// setupConstructor sets everything that needs to be setup inside the constructor
func (actor *Actor) setupConstructor() error {
	if actor.actorConfig.Workers == 0 {
		actor.actorConfig.Workers = 1
	}
	if actor.actorConfig.StreamChanBuffer < 64000 {
		actor.actorConfig.StreamChanBuffer = 64000
	}

	actor.wg = sync.WaitGroup{}
	actor.subscribers = make(map[string]func(context.Context, *nats.Msg))
	actor.subscriptionChan = make(chan *nats.Msg, actor.actorConfig.StreamChanBuffer)
	actor.subscriptions = make([]*nats.Subscription, actor.actorConfig.Workers)
	actor.downstreams = make([]string, 0)

	actor.setupLoggers()

	err := actor.setupStream()
	if err != nil {
		actor.errorLogger.Caller().
			Str("method", "setupConstructor()").
			Err(err).Msg("failed to setup stream")
	}

	return err
}

// setupLoggers
func (actor *Actor) setupLoggers() {
	outLog := zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).With().Timestamp().Logger()
	errLog := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}).With().Timestamp().Logger()
	dbgLog := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}).With().Timestamp().Logger()

	actor.infoLogger = outLog.Info().
		Str("stream.name", actor.streamName).
		Str("stream.subjects", actor.subscribeSubjects())

	actor.errorLogger = errLog.Error().
		Str("stream.name", actor.streamName).
		Str("stream.subjects", actor.subscribeSubjects())

	actor.debugLogger = dbgLog.Debug().
		Str("stream.name", actor.streamName).
		Str("stream.subjects", actor.subscribeSubjects())
}

// setupStream creates a dedicated stream for this actor
func (actor *Actor) setupStream() error {
	if actor.actorConfig.StreamConfig == nil {
		actor.actorConfig.StreamConfig = &nats.StreamConfig{}
	}

	actor.actorConfig.StreamConfig.Name = actor.streamName
	actor.actorConfig.StreamConfig.Subjects = append(actor.actorConfig.StreamConfig.Subjects, actor.subscribeSubjects())

	_, err := actor.jc().AddStream(actor.actorConfig.StreamConfig)
	if err != nil {
		if err.Error() == "nats: stream name already in use" {
			_, err = actor.jc().UpdateStream(actor.actorConfig.StreamConfig)
		}

		if err != nil {
			actor.errorLogger.Caller().Err(err).
				Str("method", "setupStream()").
				Msg("failed to create or get a stream")

			return err
		}
	}

	// Setup unsubscribe handler
	actor.subscribers["UNSUB"] = func(context.Context, *nats.Msg) {
		actor.unsubscribeAll()
	}

	return nil
}

// subscribeSubjects is usually the streamName followed by any matching characters.
// Example: stream-name.>. The greater than symbol means match more than one subpaths.
// NATS subpaths are delimited with dots.
func (actor *Actor) subscribeSubjects() string {
	return actor.streamName + ".>"
}

// jc gets the JetStreamContext
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

// commandFromKey checks if the nats key has a command.
// The nats key looks like this: stream-name.optional-key.command:POST|PUT|DELETE.
func (actor *Actor) commandFromKey(key string) string {
	chunks := strings.Split(key, ".command:")
	if len(chunks) >= 2 {
		return chunks[1]
	}

	return ""
}

// unsubscribeOne worker from stream
func (actor *Actor) unsubscribeOne(i int) error {
	sub := actor.subscriptions[i]
	if sub != nil {
		return sub.Unsubscribe()
	}

	return nil
}

// unsubscribeAll workers from stream
func (actor *Actor) unsubscribeAll() error {
	for _, sub := range actor.subscriptions {
		if sub != nil {
			sub.Unsubscribe()
		}
	}

	return nil
}

// GetStreamName returns the stream name that the actor subscribed to.
func (actor *Actor) GetStreamName() string {
	return actor.streamName
}

// SetOnConfigUpdate
func (actor *Actor) SetOnConfigUpdate(handler func(context.Context, *nats.Msg)) {
	actor.subscribers["UPDATE"] = handler
}

// SetOnConfigDelete
func (actor *Actor) SetOnConfigDelete(handler func(context.Context, *nats.Msg)) {
	actor.subscribers["DELETE"] = handler
}

// SetOnConfigDelete
func (actor *Actor) SetDownstreams(downstreams ...string) {
	actor.downstreams = downstreams
}

// PublishConfig data into JetStream with a nats key.
// The nats key looks like this: stream-name.optional-key.command:POST|PUT|DELETE.
func (actor *Actor) PublishConfig(key string, data []byte) error {
	_, err := actor.jc().Publish(key, data)
	if err != nil {
		actor.errorLogger.Caller().Err(err).
			Str("publish.key", key).
			Msg("failed to publish to JetStream")
	}

	return err
}

// runSubscriberOnce executes the subscriber handler once synchronously
func (actor *Actor) runSubscriberOnce(ctx context.Context, msg *nats.Msg) {
	subscriber, ok := actor.subscribers[actor.commandFromKey(msg.Subject)]
	if ok {
		subscriber(ctx, msg)
	}
}

// RunConfigListener listens to config changes and execute hooks
func (actor *Actor) RunConfigListener(ctx context.Context) {
	actor.infoLogger.Msg("subscribing to nats subjects")

	var err error
	var sub *nats.Subscription

	for i := 0; i < actor.actorConfig.Workers; i++ {
		actor.wg.Add(1)

		switch actor.actorConfig.StreamConfig.Retention {
		case nats.WorkQueuePolicy:
			sub, err = actor.jc().ChanQueueSubscribe(actor.subscribeSubjects(), "workers", actor.subscriptionChan)
		default:
			sub, err = actor.jc().ChanSubscribe(actor.subscribeSubjects(), actor.subscriptionChan)
		}

		if err == nil {
			actor.subscriptions[i] = sub
		} else {
			actor.errorLogger.Err(err).Msg("failed to subscribe to subjects")
			return
		}

		go func(i int) {
			defer actor.wg.Done()

			for {
				select {
				case <-ctx.Done():
					actor.debugLogger.Bool("ctx.done", true).Msg("received exit signal from the user. Exiting for loop")

					if actor.onDoneSubscribing != nil {
						err = actor.onDoneSubscribing()
						if err != nil {
							actor.debugLogger.Bool("ctx.done", true).Msg("failed to run onDoneSubscribing()")
						}
					}

					err = actor.unsubscribeOne(i)
					if err != nil {
						actor.debugLogger.Bool("ctx.done", true).Msg("failed to unsubscribe from stream")
					}

					return

				case msg := <-actor.subscriptionChan:
					actor.runSubscriberOnce(ctx, msg)
				}
			}
		}(i)
	}
	actor.wg.Wait()
}

// OnBootLoadConfig loads the configuration to setup the underlying object
func (actor *Actor) OnBootLoadConfig() error {
	actor.debugLogger.Msg("OnBootLoadConfig() is not implemented")
	return nil
}

// ServeHTTP supports updating and deleting object configuration via HTTP.
// Supported commands are POST, PUT, DELETE, and UNSUB
// HTTP GET should only be supported by the underlying object.
// Override this method if you want to do something custom.
func (actor *Actor) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	configJSONBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http_helpers.RenderJSONError(actor.errorLogger, w, r, err, http.StatusInternalServerError)
		return
	}

	command := ""

	// Normalize command
	switch r.Method {
	case "POST", "PUT":
		switch r.FormValue("command") {
		case "UNSUB":
			command = "UNSUB"
		default:
			command = "UPDATE"
		}
	case "DELETE":
		command = r.Method
	}

	// Update KV store
	switch command {
	case "UPDATE":
		revision, err := actor.kv().Put(actor.streamName, configJSONBytes)
		if err != nil {
			actor.errorLogger.Err(err).Msg("failed to update config in KV store")
			http_helpers.RenderJSONError(actor.errorLogger, w, r, err, http.StatusInternalServerError)
			return

		} else {
			actor.infoLogger.Int64("revision", int64(revision)).Msg("updated config in KV store")
		}

	case "DELETE":
		err := actor.kv().Delete(actor.keyWithoutCommand(actor.streamName))
		if err != nil {
			actor.errorLogger.Err(err).Msg("failed to delete config in KV store")
			http_helpers.RenderJSONError(actor.errorLogger, w, r, err, http.StatusInternalServerError)
			return
		}
	}

	// Push config to listeners
	err = actor.PublishConfig(actor.keyWithCommand(actor.streamName, command), configJSONBytes)
	if err != nil {
		http_helpers.RenderJSONError(actor.errorLogger, w, r, err, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write([]byte(`{"status":"success"}`))
}
