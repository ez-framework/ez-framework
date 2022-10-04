package actors

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/recws-org/recws"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/diode"
)

// WSActorConfig
type WSActorConfig struct {
	// Workers is the number of workers for this actor
	Workers int

	// WSURL is the ws:// address to connect to
	WSURL string

	WSConfig recws.RecConn
	// One day we may add KV store here
}

func NewWorkerWSActor(config WSActorConfig) (*WorkerWSActor, error) {
	actor := &WorkerWSActor{}

	config.WSConfig.Dial(config.WSURL, nil)

	actor.wsConn = config.WSConfig
	actor.config = config
	actor.wg = sync.WaitGroup{}
	actor.subscribers = make(map[string]func(context.Context, *nats.Msg))

	actor.setupLoggers()

	return actor, nil
}

// WorkerWSActor receives parameters over websocket and execute work
// It does not inherit from Actor because it doesn't connect to a Nats.
type WorkerWSActor struct {
	config      WSActorConfig
	wsConn      recws.RecConn
	wg          sync.WaitGroup
	subscribers map[string]func(context.Context, *nats.Msg)
	infoLogger  zerolog.Logger
	errorLogger zerolog.Logger
	debugLogger zerolog.Logger
}

// setupLoggers
func (actor *WorkerWSActor) setupLoggers() {
	outWriter := diode.NewWriter(os.Stdout, 1000, 0, nil)
	errWriter := diode.NewWriter(os.Stderr, 1000, 0, nil)

	actor.infoLogger = zerolog.New(zerolog.ConsoleWriter{Out: outWriter, TimeFormat: time.RFC3339}).With().Timestamp().Logger()
	actor.errorLogger = zerolog.New(zerolog.ConsoleWriter{Out: errWriter, TimeFormat: time.RFC3339}).With().Timestamp().Logger()
	actor.debugLogger = zerolog.New(zerolog.ConsoleWriter{Out: errWriter, TimeFormat: time.RFC3339}).With().Timestamp().Logger()
}

func (actor *WorkerWSActor) log(lvl zerolog.Level) *zerolog.Event {
	switch lvl {
	case zerolog.ErrorLevel:
		return actor.errorLogger.Error().
			Str("ws.url", actor.wsConn.GetURL())

	case zerolog.DebugLevel:
		return actor.debugLogger.Debug().
			Str("ws.url", actor.wsConn.GetURL())

	default:
		return actor.infoLogger.Info().
			Str("ws.url", actor.wsConn.GetURL())
	}
}

// commandFromKey checks if the nats key has a command.
// The nats key looks like this: stream-name.optional-key.command:UPDATE|DELETE.
func (actor *WorkerWSActor) commandFromKey(key string) string {
	chunks := strings.Split(key, ".command:")
	if len(chunks) >= 2 {
		return chunks[1]
	}

	return ""
}

// Close
func (actor *WorkerWSActor) Close() {
	actor.wsConn.Close()
}

// SetOnConfigUpdate
func (actor *WorkerWSActor) SetOnConfigUpdate(handler func(context.Context, *nats.Msg)) {
	actor.subscribers["UPDATE"] = handler
}

// SetOnConfigDelete
func (actor *WorkerWSActor) SetOnConfigDelete(handler func(context.Context, *nats.Msg)) {
	actor.subscribers["DELETE"] = handler
}

// runSubscriberOnce executes the subscriber handler once synchronously
func (actor *WorkerWSActor) runSubscriberOnce(ctx context.Context, msg *nats.Msg) {
	subscriber, ok := actor.subscribers[actor.commandFromKey(msg.Subject)]
	if ok {
		subscriber(ctx, msg)
	}
}

// RunConfigListener listens to config changes and update the storage
func (actor *WorkerWSActor) RunConfigListener(ctx context.Context) {
	actor.log(zerolog.InfoLevel).Msg("subscribing to websocket")

	for i := 0; i < actor.config.Workers; i++ {
		actor.wg.Add(1)

		go func(i int) {
			defer actor.wg.Done()

			actor.log(zerolog.InfoLevel).Int("index", i).Msg("running a websocket consumer")

			for {
				select {
				case <-ctx.Done():
					actor.log(zerolog.DebugLevel).
						Bool("ctx.done", true).
						Msg("received exit signal from the user. Exiting for loop")
					actor.Close()
					return

				default:
					if !actor.wsConn.IsConnected() {
						// Make sure we don't keep looping every tick when the connection is disconnected.
						time.Sleep(actor.wsConn.RecIntvlMin)
						continue
					}

					_, configWithEnvelopeBytes, err := actor.wsConn.ReadMessage()
					if err != nil {
						actor.log(zerolog.ErrorLevel).Err(err).Msg("failed to read config + envelope from websocket")
						continue
					}

					msg := &nats.Msg{}

					err = json.Unmarshal(configWithEnvelopeBytes, &msg)
					if err != nil {
						actor.log(zerolog.ErrorLevel).Err(err).Msg("failed to unmarshal config + envelope from websocket")
						continue
					}

					actor.runSubscriberOnce(ctx, msg)
				}
			}
		}(i)
	}
	actor.wg.Wait()
}
