package actors

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/websocket"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"

	"github.com/ez-framework/ez-framework/http_helpers"
)

// upgrader is the setting passed when we upgrade the websocket HTTP connection.
// We don't buffer so that downstreams can get config very quickly.
var upgrader = websocket.Upgrader{
	ReadBufferSize:  0,
	WriteBufferSize: 0,
}

// NewConfigWSServerActor is the constructor for *ConfigWSServerActor
func NewConfigWSServerActor(actorConfig ActorConfig) (*ConfigWSServerActor, error) {
	name := "ez-config-ws"

	actor := &ConfigWSServerActor{
		Actor: Actor{
			actorConfig: actorConfig,
			streamName:  name,
			infoLogger:  log.Info().Str("stream.name", name).Caller(),
			errorLogger: log.Error().Str("stream.name", name).Caller(),
			debugLogger: log.Debug().Str("stream.name", name).Caller(),
			ConfigKV:    actorConfig.ConfigKV,
		},
		configReceiverChan: make(chan []byte),
	}

	err := actor.setupStream()
	if err != nil {
		return nil, err
	}

	actor.SetPOSTSubscriber(actor.updateHandler)
	actor.SetPUTSubscriber(actor.updateHandler)
	actor.SetDELETESubscriber(actor.deleteHandler)

	return actor, nil
}

// ConfigWSServerActor listens to changes and push all config to WS clients
type ConfigWSServerActor struct {
	Actor
	configReceiverChan chan []byte
}

func (actor *ConfigWSServerActor) updateHandler(msg *nats.Msg) {
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
}

// deleteHandler listens to DELETE command and do something
func (actor *ConfigWSServerActor) deleteHandler(msg *nats.Msg) {
	err := actor.unsubscribeFromOnConfigUpdate()
	if err != nil {
		actor.errorLogger.Err(err).
			Err(err).
			Str("subjects", actor.subscribeSubjects()).
			Msg("failed to unsubscribe from subjects")
	}
}

// ServeHTTP is a websocket HTTTP handler.
// It receives websocket connections and then pushes config data to websocket clients.
func (actor *ConfigWSServerActor) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		http_helpers.RenderJSONError(actor.errorLogger, w, r, err, http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	actor.debugLogger.Msg("received a websocket connection")

	for {
		configWithEnvelopeBytes := <-actor.configReceiverChan

		err = conn.WriteMessage(websocket.TextMessage, configWithEnvelopeBytes)
		if err != nil {
			actor.errorLogger.Err(err).Msg("failed to push config to websocket clients")
		}
	}
}
