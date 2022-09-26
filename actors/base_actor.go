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
	"github.com/ez-framework/ez-framework/http_logs"
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

	LogsStreamConfig *nats.StreamConfig

	// ConfigKV is the KV store available for all actors.
	ConfigKV *configkv.ConfigKV
}

// IActor is the interface to conform to for all actors
type IActor interface {
	GetStreamName() string

	RunSubscribersBlocking(context.Context)
	RunSubscriberOnce(msg *nats.Msg)
	Unsubscribe() error

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

	streamName       string
	actorConfig      ActorConfig
	wg               sync.WaitGroup
	postSubscriber   func(msg *nats.Msg)
	putSubscriber    func(msg *nats.Msg)
	deleteSubscriber func(msg *nats.Msg)
	unsubSubscriber  func(msg *nats.Msg)
	subscriptionChan chan *nats.Msg
	subscription     *nats.Subscription
	infoLogger       *zerolog.Event
	errorLogger      *zerolog.Event
	debugLogger      *zerolog.Event
}

// setupConstructor sets everything that needs to be setup inside the constructor
func (actor *Actor) setupConstructor() error {
	actor.wg = sync.WaitGroup{}
	actor.subscriptionChan = make(chan *nats.Msg)

	if actor.actorConfig.Workers == 0 {
		actor.actorConfig.Workers = 1
	}

	actor.setupLoggers()

	err := actor.setupStream()
	if err != nil {
		actor.errorLogger.Caller().Err(err).Msg("failed to setup stream")
	}

	return err
}

// setupLoggers
func (actor *Actor) setupLoggers() {
	var infoWriter io.Writer
	var errorWriter io.Writer
	var debugWriter io.Writer

	infoWriter = os.Stdout
	errorWriter = os.Stderr
	debugWriter = os.Stderr

	// if actor.actorConfig.LogsStreamConfig, we also pipe logs to Nats
	if actor.actorConfig.LogsStreamConfig != nil {
		for _, logLevel := range []string{"info", "error", "debug"} {
			natsLogger, err := http_logs.NewHTTPLogs(http_logs.HTTPLogsConfig{
				LogLevel:         logLevel,
				JetStreamContext: actor.jc(),
				StreamConfig:     actor.actorConfig.LogsStreamConfig,
			})
			if err != nil {
				actor.errorLogger.Err(err).Msg("failed to create nats logger")
				continue
			}

			switch logLevel {
			case "info":
				infoWriter = io.MultiWriter(os.Stdout, natsLogger)
			case "error":
				errorWriter = io.MultiWriter(os.Stderr, natsLogger)
			case "debug":
				debugWriter = io.MultiWriter(os.Stderr, natsLogger)
			}
		}
	}

	outLog := zerolog.New(zerolog.ConsoleWriter{Out: infoWriter, TimeFormat: time.RFC3339}).With().Timestamp().Logger()
	errLog := zerolog.New(zerolog.ConsoleWriter{Out: errorWriter, TimeFormat: time.RFC3339}).With().Timestamp().Logger()
	dbgLog := zerolog.New(zerolog.ConsoleWriter{Out: debugWriter, TimeFormat: time.RFC3339}).With().Timestamp().Logger()

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
				Msg("failed to create or get a stream")

			return err
		}
	}

	// Setup unsubscribe handler
	actor.unsubSubscriber = func(msg *nats.Msg) {
		actor.Unsubscribe()
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

// Unsubscribe from stream
func (actor *Actor) Unsubscribe() error {
	if actor.subscription != nil {
		return actor.subscription.Unsubscribe()
	}

	return nil
}

// GetStreamName returns the stream name that the actor subscribed to.
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

	return err
}

// RunSubscriberOnce executes the subscriber handler once synchronously
func (actor *Actor) RunSubscriberOnce(msg *nats.Msg) {
	switch {
	case actor.keyHasCommand(msg.Subject, "POST"):
		if actor.postSubscriber != nil {
			actor.postSubscriber(msg)
		}
	case actor.keyHasCommand(msg.Subject, "PUT"):
		if actor.putSubscriber != nil {
			actor.putSubscriber(msg)
		}
	case actor.keyHasCommand(msg.Subject, "DELETE"):
		if actor.deleteSubscriber != nil {
			actor.deleteSubscriber(msg)
		}
	case actor.keyHasCommand(msg.Subject, "UNSUB"):
		if actor.unsubSubscriber != nil {
			actor.unsubSubscriber(msg)
		}
	}
}

// RunSubscribersBlocking listens to config changes and execute hooks
func (actor *Actor) RunSubscribersBlocking(ctx context.Context) {
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
			actor.subscription = sub
		} else {
			actor.errorLogger.Err(err).Msg("failed to subscribe to subjects")
			return
		}

		go func() {
			for {
				select {
				case <-ctx.Done():
					actor.wg.Done()
					return

				case msg := <-actor.subscriptionChan:
					actor.RunSubscriberOnce(msg)
				}
			}
		}()
	}
	actor.wg.Wait()
}

// OnBootLoadConfig
func (actor *Actor) OnBootLoadConfig() error {
	actor.infoLogger.Msg("OnBootLoadConfig() is not implemented")
	return nil
}

// ServeHTTP supports updating, deleting, and subscribing via HTTP.
// Actor's HTTP handler always support only POST, PUT, DELETE, and UNSUB
// HTTP GET should only be supported by the underlying struct.
// Override this method if you want to do something custom.
func (actor *Actor) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	configJSONBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http_helpers.RenderJSONError(actor.errorLogger, w, r, err, http.StatusInternalServerError)
		return
	}

	command := ""

	switch r.Method {
	case "POST":
		command = r.FormValue("command")
	case "DELETE":
		command = r.Method
	}

	err = actor.Publish(actor.keyWithCommand(actor.streamName, command), configJSONBytes)
	if err != nil {
		http_helpers.RenderJSONError(actor.errorLogger, w, r, err, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write([]byte(`{"status":"success"}`))
}
