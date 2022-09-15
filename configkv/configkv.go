package configkv

import (
	"os"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/ez-framework/ez-framework/config_internal"
)

var configKVLogger = log.With().
	Str("KVBucket", config_internal.KVBucketName).
	Logger().Output(zerolog.ConsoleWriter{Out: os.Stderr})

// NewConfigKV is the constructor for *ConfigKV
func NewConfigKV(jetstreamContext nats.JetStreamContext) (*ConfigKV, error) {
	configkv := &ConfigKV{
		jetstreamctx: jetstreamContext,
	}

	err := configkv.setupConfigKVStore()
	if err != nil {
		return nil, err
	}

	return configkv, nil
}

type ConfigKV struct {
	jetstreamctx nats.JetStreamContext
	KV           nats.KeyValue
}

func (configkv *ConfigKV) setupConfigKVStore() error {
	kv, err := configkv.jetstreamctx.KeyValue(config_internal.KVBucketName)
	if err == nil {
		configkv.KV = kv
		return nil
	}

	kv, err = configkv.jetstreamctx.CreateKeyValue(&nats.KeyValueConfig{Bucket: config_internal.KVBucketName})
	if err == nil {
		configkv.KV = kv
		return nil
	}

	configKVLogger.Error().Err(err).Msg("Failed to setup KV store")
	return err
}

// GetConfigBytes returns config from the KV backend in bytes
func (configkv *ConfigKV) GetConfigBytes(key string) ([]byte, error) {
	entry, err := configkv.KV.Get(key)
	if err != nil {
		configKVLogger.Error().Err(err).Str("configKey", key).Msg("Failed to get config from KV store")
		return nil, err
	}

	return entry.Value(), nil
}
