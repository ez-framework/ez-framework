package configkv

import (
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
)

// NewConfigKV is the constructor for *ConfigKV
func NewConfigKV(jetstreamContext nats.JetStreamContext) (*ConfigKV, error) {
	configkv := &ConfigKV{
		jc:         jetstreamContext,
		bucketName: "ez-configkv",
	}

	err := configkv.setupConfigKVStore()
	if err != nil {
		return nil, err
	}

	return configkv, nil
}

type ConfigKV struct {
	jc         nats.JetStreamContext
	bucketName string
	KV         nats.KeyValue
}

func (configkv *ConfigKV) setupConfigKVStore() error {
	kv, err := configkv.jc.KeyValue(configkv.bucketName)
	if err == nil {
		configkv.KV = kv
		return nil
	}

	kv, err = configkv.jc.CreateKeyValue(&nats.KeyValueConfig{Bucket: configkv.bucketName})
	if err == nil {
		configkv.KV = kv
		return nil
	}

	log.Error().Err(err).Str("bucket.name", configkv.bucketName).Msg("failed to setup KV store")
	return err
}

// GetConfigBytes returns config from the KV backend in bytes
func (configkv *ConfigKV) GetConfigBytes(key string) ([]byte, error) {
	entry, err := configkv.KV.Get(key)
	if err != nil {
		log.Error().Err(err).
			Str("bucket.name", configkv.bucketName).
			Str("config.key", key).
			Msg("failed to get config from KV store")
		return nil, err
	}

	return entry.Value(), nil
}
