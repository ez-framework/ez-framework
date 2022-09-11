package config_live

import (
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
)

func NewConfigLive(jetstreamContext nats.JetStreamContext) (*ConfigLive, error) {
	cl := &ConfigLive{
		jc: jetstreamContext,
	}

	kv, err := cl.jc.CreateKeyValue(&nats.KeyValueConfig{Bucket: "ez-configlive"})
	if err != nil {
		return nil, err
	}
	cl.kv = kv

	err = cl.createStream()
	if err != nil {
		return nil, err
	}

	return cl, nil
}

type ConfigLive struct {
	jc nats.JetStreamContext
	kv nats.KeyValue
}

func (cl *ConfigLive) createStream() error {
	streamName := "ez-configlive"
	streamSubjects := "ez-configlive.*"

	stream, err := cl.jc.StreamInfo(streamName)
	if err != nil {
		if err.Error() != "nats: stream not found" {
			return err
		}
	}
	if stream == nil {
		log.Info().Str("streamName", streamName).Str("streamSubjects", streamSubjects).Msg("Creating a stream")

		_, err = cl.jc.AddStream(&nats.StreamConfig{
			Name:     streamName,
			Subjects: []string{streamSubjects},
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (cl *ConfigLive) Publish(key string, data []byte) error {
	_, err := cl.jc.Publish(key, data)
	if err != nil {
		log.Error().Err(err).Str("configKey", key).Msg("Failed to publish config")
	}
	return err
}

// SubscribeConfigUpdate listens to config changes and update the storage
// TODO: Use the most consistent settings.
func (cl *ConfigLive) SubscribeConfigUpdate() {
	cl.jc.Subscribe("ez-configlive.*", func(msg *nats.Msg) {
		log.Info().Str("subscribeGlob", "ez-configlive.*").Msg("Subscribing to a nats subject")

		key := msg.Subject
		value := msg.Data

		revision, err := cl.kv.Put(key, value)
		if err != nil {
			log.Error().Err(err).Msg("Failed to update config")
		} else {
			log.Info().Int64("revision", int64(revision)).Str("configKey", key).Msg("Updated config")
		}
	})
}

// GetConfigBytes returns config from the KV backend in bytes.s
func (cl *ConfigLive) GetConfigBytes(key string) ([]byte, error) {
	entry, err := cl.kv.Get(key)
	if err != nil {
		log.Error().Err(err).Str("configKey", key).Msg("Failed to get config")
		return nil, err
	}

	return entry.Value(), nil
}
