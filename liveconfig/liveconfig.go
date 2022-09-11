package liveconfig

import (
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
)

func NewLiveConfig(jetstreamContext nats.JetStreamContext) (*LiveConfig, error) {
	ps := &LiveConfig{
		JetstreamContext: jetstreamContext,
	}

	kv, err := ps.JetstreamContext.CreateKeyValue(&nats.KeyValueConfig{Bucket: "ez-liveconfig"})
	if err != nil {
		return nil, err
	}
	ps.kv = kv

	return ps, nil
}

type LiveConfig struct {
	JetstreamContext nats.JetStreamContext
	kv               nats.KeyValue
}

// SubscribeConfigUpdate listens to config changes and update the storage
// TODO: Use the most consistent settings.
func (ps *LiveConfig) SubscribeConfigUpdate() (*nats.Subscription, error) {
	return ps.JetstreamContext.Subscribe("ez-liveconfig.*", func(msg *nats.Msg) {
		_, err := ps.kv.PutString("aaa", "aaa")
		if err != nil {
			log.Error().Err(err).Msg("Failed to update config")
		}
	}, nats.Durable("monitor"))
}
