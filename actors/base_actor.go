package actors

import (
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"

	"github.com/ez-framework/ez-framework/configkv"
)

type Actor struct {
	globalConfig  GlobalConfig
	jc            nats.JetStreamContext
	jetstreamName string
	ConfigKV      *configkv.ConfigKV
	infoLogger    *zerolog.Event
	errorLogger   *zerolog.Event
}

func (actor *Actor) setupJetStreamStream(streamConfig *nats.StreamConfig) error {
	actor.infoLogger.
		Str("stream.name", actor.jetstreamName).
		Msg("about to setup a new stream")

	streamConfig.Name = actor.jetstreamName
	streamConfig.Subjects = append(streamConfig.Subjects, actor.jetstreamName+".>")

	_, err := actor.jc.AddStream(streamConfig)
	if err != nil {
		actor.errorLogger.Err(err).
			Str("stream.name", actor.jetstreamName).
			Msg("failed to create or get a stream")

		return err
	}

	return nil
}

func (configactor *Actor) kv() nats.KeyValue {
	return configactor.ConfigKV.KV
}

// keyWithoutCommand strips the command which is appended at the end
func (actor *Actor) keyWithoutCommand(key string) string {
	keyEndIndex := strings.Index(key, ".command:")
	return key[0:keyEndIndex]
}

func (actor *Actor) keyWithCommand(key, command string) string {
	return key + ".command:" + command
}

func (actor *Actor) keyHasCommand(key, command string) bool {
	return strings.HasSuffix(key, ".command:"+command)
}

// Publish a new config by passing it into JetStream with configKey identifier
func (actor *Actor) Publish(key string, data []byte) error {
	_, err := actor.jc.Publish(key, data)
	if err != nil {
		actor.errorLogger.Err(err).
			Str("publish.key", key).
			Msg("failed to publish to JetStream")
	}

	return err
}
