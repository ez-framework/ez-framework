package actors

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/diode"

	"github.com/ez-framework/ez-framework/configkv"
	"github.com/ez-framework/ez-framework/http_helpers"
)

type ActorNatsConfig struct {
	// Addr is the address to connect to
	Addr string

	// Conn is the connection to a NATS cluster
	Conn *nats.Conn

	// JetStreamContext
	JetStreamContext nats.JetStreamContext

	// StreamConfig
	StreamConfig *nats.StreamConfig

	// StreamChanBuffer, min is 64000
	StreamChanBuffer int
}

// ActorConfig is the config that all actors need
type ActorConfig struct {
	// Workers is the number of workers for this actor
	Workers int

	// HTTPAddr is the address to bind the HTTP server
	HTTPAddr string

	// Configuration for Nats
	Nats ActorNatsConfig

	// ConfigKV is the KV store available for all actors.
	ConfigKV *configkv.ConfigKV
}

// IActor is the interface to conform to for all actors
type IActor interface {
	// GetStreamName
	GetStreamName() string

	// RunConfigListener
	RunConfigListener(context.Context)

	// PublishConfig
	PublishConfig(string, []byte) error

	// ServeHTTP
	ServeHTTP(http.ResponseWriter, *http.Request)

	// OnBootLoadConfig
	OnBootLoadConfig() error

	// SetOnConfigUpdate
	SetOnConfigUpdate(func(context.Context, *nats.Msg))

	// SetOnConfigDelete
	SetOnConfigDelete(func(context.Context, *nats.Msg))

	// SetDownstreams
	SetDownstreams(...string)

	// setupStream
	setupStream() error

	// jsc
	jsc() nats.JetStreamContext

	// kv
	kv() nats.KeyValue

	// unsubscribeAll
	unsubscribeAll() error
}

// Actor is the base struct for all actors.
// It provides common helper functions and conforms to IActor.
type Actor struct {
	ConfigKV *configkv.ConfigKV

	streamName       string
	config           ActorConfig
	wg               sync.WaitGroup
	subscribers      map[string]func(context.Context, *nats.Msg)
	downstreams      []string
	subscriptionChan chan *nats.Msg
	subscriptions    []*nats.Subscription
	infoLogger       zerolog.Logger
	errorLogger      zerolog.Logger
	debugLogger      zerolog.Logger
}

// setupConstructor sets everything that needs to be setup inside the constructor
func (actor *Actor) setupConstructor() error {
	if actor.config.Workers == 0 {
		actor.config.Workers = 1
	}

	// We want to set this really high to avoid Nats giving us slow subscriber error
	if actor.config.Nats.StreamChanBuffer < 64000 {
		actor.config.Nats.StreamChanBuffer = 64000
	}

	actor.wg = sync.WaitGroup{}
	actor.subscribers = make(map[string]func(context.Context, *nats.Msg))
	actor.subscriptionChan = make(chan *nats.Msg, actor.config.Nats.StreamChanBuffer)
	actor.subscriptions = make([]*nats.Subscription, actor.config.Workers)
	actor.downstreams = make([]string, 0)

	actor.setupLoggers()

	err := actor.setupStream()
	if err != nil {
		actor.log(zerolog.ErrorLevel).Caller().
			Str("method", "setupConstructor()").
			Err(err).Msg("failed to setup stream")
	}

	return err
}

// setupLoggers
func (actor *Actor) setupLoggers() {
	outWriter := diode.NewWriter(os.Stdout, 1000, 0, nil)
	errWriter := diode.NewWriter(os.Stderr, 1000, 0, nil)

	actor.infoLogger = zerolog.New(zerolog.ConsoleWriter{Out: outWriter, TimeFormat: time.RFC3339}).With().Timestamp().Logger()
	actor.errorLogger = zerolog.New(zerolog.ConsoleWriter{Out: errWriter, TimeFormat: time.RFC3339}).With().Timestamp().Logger()
	actor.debugLogger = zerolog.New(zerolog.ConsoleWriter{Out: errWriter, TimeFormat: time.RFC3339}).With().Timestamp().Logger()
}

func (actor *Actor) log(lvl zerolog.Level) *zerolog.Event {
	switch lvl {
	case zerolog.ErrorLevel:
		return actor.errorLogger.Error().
			Str("stream.name", actor.streamName).
			Str("stream.subjects", actor.subscribeSubjects())

	case zerolog.DebugLevel:
		return actor.debugLogger.Debug().
			Str("stream.name", actor.streamName).
			Str("stream.subjects", actor.subscribeSubjects())
	default:
		return actor.infoLogger.Info().
			Str("stream.name", actor.streamName).
			Str("stream.subjects", actor.subscribeSubjects())
	}
}

// setupStream creates a dedicated stream for this actor
func (actor *Actor) setupStream() error {
	if actor.config.Nats.StreamConfig == nil {
		actor.config.Nats.StreamConfig = &nats.StreamConfig{}
	}

	actor.config.Nats.StreamConfig.Name = actor.streamName
	actor.config.Nats.StreamConfig.Subjects = append(actor.config.Nats.StreamConfig.Subjects, actor.subscribeSubjects())

	_, err := actor.jsc().AddStream(actor.config.Nats.StreamConfig)
	if err != nil {
		if err.Error() == "nats: stream name already in use" {
			_, err = actor.jsc().UpdateStream(actor.config.Nats.StreamConfig)
		}

		if err != nil {
			actor.log(zerolog.ErrorLevel).Caller().Err(err).
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
func (actor *Actor) jsc() nats.JetStreamContext {
	return actor.config.Nats.JetStreamContext
}

// kv gets the underlying KV store
func (actor *Actor) kv() nats.KeyValue {
	return actor.ConfigKV.KV
}

// flush Nats connection
func (actor *Actor) flush() error {
	return actor.config.Nats.Conn.Flush()
}

// keyWithCommand appends the command at the end of the key.
// The nats key looks like this: stream-name.optional-key.command:UPDATE|DELETE.
func (actor *Actor) keyWithCommand(key, command string) string {
	return key + ".command:" + command
}

// keyWithoutCommand strips the command which is appended at the end.
// The nats key looks like this: stream-name.optional-key.command:UPDATE|DELETE.
func (actor *Actor) keyWithoutCommand(key string) string {
	keyEndIndex := strings.Index(key, ".command:")
	return key[0:keyEndIndex]
}

// keyHasCommand checks if the nats key has a command.
// The nats key looks like this: stream-name.optional-key.command:UPDATE|DELETE.
// func (actor *Actor) keyHasCommand(key, command string) bool {
// 	return strings.HasSuffix(key, ".command:"+command)
// }

// commandFromKey checks if the nats key has a command.
// The nats key looks like this: stream-name.optional-key.command:UPDATE|DELETE.
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
// The nats key looks like this: stream-name.optional-key.command:UPDATE|DELETE.
func (actor *Actor) PublishConfig(key string, data []byte) error {
	_, err := actor.jsc().Publish(key, data)
	if err != nil {
		actor.log(zerolog.ErrorLevel).Caller().Err(err).
			Str("publish.key", key).
			Msg("failed to publish to JetStream")
	}

	// Flush the message immediately to prevent delay
	err = actor.flush()

	return err
}

// runSubscriberOnce executes the subscriber handler once synchronously
func (actor *Actor) runSubscriberOnce(ctx context.Context, msg *nats.Msg) {
	subscriber, ok := actor.subscribers[actor.commandFromKey(msg.Subject)]
	if ok {
		err := msg.AckSync()
		if err == nil {
			subscriber(ctx, msg)
		}
	}
}

// RunConfigListener listens to config changes and execute hooks
func (actor *Actor) RunConfigListener(ctx context.Context) {
	actor.log(zerolog.InfoLevel).Msg("subscribing to nats subjects")

	var err error
	var sub *nats.Subscription

	for i := 0; i < actor.config.Workers; i++ {
		actor.wg.Add(1)

		switch actor.config.Nats.StreamConfig.Retention {
		case nats.WorkQueuePolicy:
			sub, err = actor.jsc().ChanQueueSubscribe(actor.subscribeSubjects(), "workers", actor.subscriptionChan)
		default:
			sub, err = actor.jsc().ChanSubscribe(actor.subscribeSubjects(), actor.subscriptionChan)
		}

		if err == nil {
			actor.subscriptions[i] = sub
		} else {
			actor.errorLogger.Err(err).Msg("failed to subscribe to nats subjects")
			return
		}

		go func(i int) {
			defer actor.wg.Done()

			for {
				select {
				case <-ctx.Done():
					actor.log(zerolog.DebugLevel).
						Int("worker.index", i).
						Bool("ctx.done", true).
						Msg("received exit signal from the user. Exiting for loop")

					err = actor.unsubscribeOne(i)
					if err != nil {
						actor.log(zerolog.ErrorLevel).Caller().Err(err).
							Int("worker.index", i).
							Bool("ctx.done", true).
							Msg("failed to unsubscribe from stream")
					}

					return

				case msg := <-actor.subscriptionChan:
					actor.log(zerolog.InfoLevel).
						Int("worker.index", i).
						Msg("consuming a configuration")
					actor.runSubscriberOnce(ctx, msg)
				}
			}
		}(i)
	}
	actor.wg.Wait()
}

// OnBootLoadConfig loads the configuration to setup the underlying object
func (actor *Actor) OnBootLoadConfig() error {
	actor.log(zerolog.DebugLevel).Msg("OnBootLoadConfig() is not implemented")
	return nil
}

// configPut saves config
func (actor *Actor) configPut(key string, value []byte) (uint64, error) {
	return actor.kv().Put(key, value)
}

// configDelete delete config
func (actor *Actor) configDelete(key string) error {
	return actor.kv().Delete(key)
}

func (actor *Actor) normalizeCommandFromHTTP(r *http.Request) string {
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

	return command
}

// ServeHTTP supports updating and deleting object configuration via HTTP.
// Supported commands are POST, PUT, DELETE, and UNSUB
// HTTP GET should only be supported by the underlying object.
// Override this method if you want to do something custom.
func (actor *Actor) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	configJSONBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http_helpers.RenderJSONError(actor.log(zerolog.ErrorLevel), w, r, err, http.StatusInternalServerError)
		return
	}

	command := actor.normalizeCommandFromHTTP(r)

	// Update KV store
	switch command {
	case "UPDATE":
		if !bytes.Equal(configJSONBytes, []byte(`{}`)) {
			revision, err := actor.configPut(actor.streamName, configJSONBytes)
			if err != nil {
				actor.log(zerolog.ErrorLevel).Err(err).Msg("failed to update config in KV store")
				http_helpers.RenderJSONError(actor.log(zerolog.ErrorLevel), w, r, err, http.StatusInternalServerError)
				return

			} else {
				actor.log(zerolog.InfoLevel).Int64("revision", int64(revision)).Msg("updated config in KV store")
			}
		}

	case "DELETE":
		err := actor.configDelete(actor.keyWithoutCommand(actor.streamName))
		if err != nil {
			actor.log(zerolog.ErrorLevel).Err(err).Msg("failed to delete config in KV store")
			http_helpers.RenderJSONError(actor.log(zerolog.ErrorLevel), w, r, err, http.StatusInternalServerError)
			return
		}
	}

	// Push config to listeners
	err = actor.PublishConfig(actor.keyWithCommand(actor.streamName, command), configJSONBytes)
	if err != nil {
		http_helpers.RenderJSONError(actor.log(zerolog.ErrorLevel), w, r, err, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write([]byte(`{"status":"success"}`))
}
