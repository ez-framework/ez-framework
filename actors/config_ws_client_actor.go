package actors

import (
	"encoding/json"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog/log"
)

// NewConfigWSClientActor is the constructor for *ConfigWSClientActor
func NewConfigWSClientActor(settings IConfigWSClientActorSettings) (*ConfigWSClientActor, error) {
	actor := &ConfigWSClientActor{
		Actor: Actor{
			infoLogger:  log.Info(),
			errorLogger: log.Error(),
		},
		settings: settings,
		kv:       settings.GetKV(),
	}

	// TODO: Always try to reconnect
	conn, _, err := websocket.DefaultDialer.Dial(settings.GetWSURL(), nil)
	if err != nil {
		return nil, err
	}
	actor.wsConn = conn

	return actor, nil
}

// ConfigWSClientActor listens to changes and push all config to WS clients
type ConfigWSClientActor struct {
	Actor
	settings IConfigWSClientActorSettings
	wsConn   *websocket.Conn
	kv       IPutDelete
}

// Run listens to config changes and update the storage
func (actor *ConfigWSClientActor) Run() {
	actor.infoLogger.Caller().Msg("subscribing to websocket")

	for {
		_, configWithEnvelopeBytes, err := actor.wsConn.ReadMessage()
		if err != nil {
			actor.errorLogger.Err(err).Msg("failed to read config + envelope from websocket")
			continue
		}

		configWithEnvelope := ConfigWSActorPayload{}

		err = json.Unmarshal(configWithEnvelopeBytes, &configWithEnvelope)
		if err != nil {
			actor.errorLogger.Err(err).Msg("failed to unmarshal config + envelope from websocket")
			continue
		}

		configBytes, err := json.Marshal(configWithEnvelope.Body)
		if err != nil {
			actor.errorLogger.Err(err).Msg("failed to marshal config from websocket")
			continue
		}

		switch configWithEnvelope.Method {
		case "POST", "PUT":
			for key, value := range configWithEnvelope.Body {
				valueBytes, err := json.Marshal(value)
				if err != nil {
					actor.errorLogger.Err(err).Msg("failed to marshal config content before saving")
					continue
				}

				err = actor.kv.Put(key, valueBytes)
				if err != nil {
					actor.errorLogger.Err(err).Msg("failed to save config content")
					continue
				}
			}

			for _, c := range actor.settings.GetOnPutChannels() {
				c <- configBytes
			}

		case "DELETE":
			for key, _ := range configWithEnvelope.Body {
				err = actor.kv.Delete(key)
				if err != nil {
					actor.errorLogger.Err(err).Msg("failed to delete config content")
					continue
				}
			}

			for _, c := range actor.settings.GetOnDeleteChannels() {
				c <- configBytes
			}
		}
	}
}
