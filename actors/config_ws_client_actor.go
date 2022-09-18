package actors

import (
	"encoding/json"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog/log"
)

type IConfigWSClientActorSettings interface {
	GetWSURL() string
	SetOnPutChannels([](chan []byte))
	SetOnDeleteChannels([](chan []byte))
	GetOnPutChannels() [](chan []byte)
	GetOnDeleteChannels() [](chan []byte)
}

type ConfigWSClientActorSettings struct {
	WSURL            string
	OnPutChannels    [](chan []byte)
	OnDeleteChannels [](chan []byte)
}

func (settings ConfigWSClientActorSettings) GetWSURL() string {
	return settings.WSURL
}

func (settings ConfigWSClientActorSettings) GetOnPutChannels() [](chan []byte) {
	return settings.OnPutChannels
}

func (settings ConfigWSClientActorSettings) GetOnDeleteChannels() [](chan []byte) {
	return settings.OnDeleteChannels
}

// NewConfigWSClientActor is the constructor for *ConfigWSClientActor
func NewConfigWSClientActor(settings IConfigWSClientActorSettings) (*ConfigWSClientActor, error) {
	actor := &ConfigWSClientActor{
		Actor: Actor{
			infoLogger:  log.Info(),
			errorLogger: log.Error(),
		},
		settings: settings,
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
			println("saving the config")

			for _, c := range actor.settings.GetOnPutChannels() {
				c <- configBytes
			}
		case "DELETE":
			println("delete the config")

			for _, c := range actor.settings.GetOnDeleteChannels() {
				c <- configBytes
			}
		}
	}
}
