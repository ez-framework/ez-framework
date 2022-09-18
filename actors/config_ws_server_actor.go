package actors

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"

	"github.com/ez-framework/ez-framework/http_helpers"
)

type IConfigWSActorPayload interface {
	GetMethod() string
	GetBody() map[string]interface{}
}

type ConfigWSActorPayload struct {
	Method string
	Body   map[string]interface{}
}

func (payload ConfigWSActorPayload) GetMethod() string { return payload.Method }

func (payload ConfigWSActorPayload) GetBody() any { return payload.Body }

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

// NewConfigWSServerActor is the constructor for *ConfigWSServerActor
func NewConfigWSServerActor(globalConfig GlobalConfig) (*ConfigWSServerActor, error) {
	name := "ez-config-ws"

	actor := &ConfigWSServerActor{
		Actor: Actor{
			jc:            globalConfig.JetStreamContext,
			jetstreamName: name,
			infoLogger:    log.Info().Str("stream.name", name),
			errorLogger:   log.Error().Str("stream.name", name),
			ConfigKV:      globalConfig.ConfigKV,
		},
		configReceiverChan: make(chan []byte),
	}

	err := actor.setupJetStreamStream(&nats.StreamConfig{
		MaxAge:    1 * time.Minute,
		Retention: nats.WorkQueuePolicy,
	})
	if err != nil {
		return nil, err
	}

	return actor, nil
}

// ConfigWSServerActor listens to changes and push all config to WS clients
type ConfigWSServerActor struct {
	Actor
	configReceiverChan chan []byte
}

func (actor *ConfigWSServerActor) jetstreamSubscribeSubjects() string {
	return actor.jetstreamName + ".>"
}

// Run listens to config changes and update the storage
func (actor *ConfigWSServerActor) Run() {
	actor.infoLogger.Caller().Msg("subscribing to nats subjects")

	actor.jc.QueueSubscribe(actor.jetstreamSubscribeSubjects(), "workers", func(msg *nats.Msg) {
		// TODO: We can strip out certain config keys in the future

		// 1. Unpack the config received
		config := make(map[string]any)

		err := json.Unmarshal(msg.Data, &config)
		if err != nil {
			actor.errorLogger.Err(err).Msg("failed to unmarshal config")
			return
		}

		// 2. Create an envelope for WS clients
		configWithEnvelope := ConfigWSActorPayload{
			Body: config,
		}

		if actor.keyHasCommand(msg.Subject, "POST") || actor.keyHasCommand(msg.Subject, "PUT") {
			configWithEnvelope.Method = "POST"

		} else if actor.keyHasCommand(msg.Subject, "DELETE") {
			configWithEnvelope.Method = "DELETE"
		}

		// 3. Push the config with the envelope to WS clients
		configWithEnvelopeBytes, err := json.Marshal(configWithEnvelope)
		if err != nil {
			actor.errorLogger.Err(err).Msg("failed to unmarshal config with websocket envelope")
			return
		}

		actor.configReceiverChan <- configWithEnvelopeBytes
	})
}

// ServeHTTP is a websocket HTTTP handler.
// It pushes all config to websocket clients.
func (actor *ConfigWSServerActor) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		http_helpers.RenderJSONError(actor.errorLogger, w, r, err, http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	actor.infoLogger.Caller().Msg("received a websocket connection")

	for {
		configWithEnvelopeBytes := <-actor.configReceiverChan

		err = conn.WriteMessage(websocket.TextMessage, configWithEnvelopeBytes)
		if err != nil {
			actor.errorLogger.Err(err).Msg("failed to push config to websocket clients")
		}
	}
}
