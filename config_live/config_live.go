package config_live

import (
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
)

func NewConfigLive(jetstreamContext nats.JetStreamContext) (*ConfigLive, error) {
	ps := &ConfigLive{
		JetstreamContext: jetstreamContext,
	}

	kv, err := ps.JetstreamContext.CreateKeyValue(&nats.KeyValueConfig{Bucket: "ez-configlive"})
	if err != nil {
		return nil, err
	}
	ps.kv = kv

	return ps, nil
}

type ConfigLive struct {
	JetstreamContext nats.JetStreamContext
	kv               nats.KeyValue
}

func (ps *ConfigLive) Publish(key string, data []byte) error {
	_, err := ps.JetstreamContext.Publish(key, data)
	if err != nil {
		log.Error().Err(err).Str("configKey", key).Msg("Failed to publish config")
	}
	return err
}

// SubscribeConfigUpdate listens to config changes and update the storage
// TODO: Use the most consistent settings.
func (ps *ConfigLive) SubscribeConfigUpdate() (*nats.Subscription, error) {
	return ps.JetstreamContext.Subscribe("ez-configlive.*", func(msg *nats.Msg) {
		key := msg.Subject
		value := msg.Data

		revision, err := ps.kv.Put(key, value)
		if err != nil {
			log.Error().Err(err).Msg("Failed to update config")
		} else {
			log.Info().Int64("revision", int64(revision)).Str("configKey", key).Msg("Updated config")
		}
	})
}

// GetConfigBytes returns config from the KV backend in bytes.s
func (ps *ConfigLive) GetConfigBytes(key string) ([]byte, error) {
	entry, err := ps.kv.Get(key)
	if err != nil {
		log.Error().Err(err).Str("configKey", key).Msg("Failed to get config")
		return nil, err
	}

	return entry.Value(), nil
}
