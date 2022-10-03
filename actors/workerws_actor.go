package actors

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/diode"
)

func NewWorkerWSActor(wsurl string) (*WorkerWSActor, error) {
	actor := &WorkerWSActor{}

	// TODO: Always try to reconnect
	conn, _, err := websocket.DefaultDialer.Dial(wsurl, nil)
	if err != nil {
		return nil, err
	}
	actor.wsURL = wsurl
	actor.wsConn = conn
	actor.subscribers = make(map[string]func(context.Context, *nats.Msg))

	actor.setupLoggers()

	return actor, nil
}

// WorkerWSActor receives parameters over websocket and execute work
// It does not inherit from Actor because it doesn't connect to a Nats.
type WorkerWSActor struct {
	wsURL       string
	wsConn      *websocket.Conn
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
			Str("ws.url", actor.wsURL)

	case zerolog.DebugLevel:
		return actor.debugLogger.Debug().
			Str("ws.url", actor.wsURL)

	default:
		return actor.infoLogger.Info().
			Str("ws.url", actor.wsURL)
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

	for {
		select {
		case <-ctx.Done():
			actor.log(zerolog.DebugLevel).
				Bool("ctx.done", true).
				Msg("received exit signal from the user. Exiting for loop")
			return

		default:
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
}
