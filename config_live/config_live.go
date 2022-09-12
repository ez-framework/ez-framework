package config_live

import (
	"bytes"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/ez-framework/ez-framework/config_internal"
)

var configLiveLogger = log.With().
	Str("streamName", config_internal.JetStreamStreamName).
	Str("streamSubjects", config_internal.JetStreamStreamSubjects).
	Str("KVBucket", config_internal.KVBucketName).
	Logger().Output(zerolog.ConsoleWriter{Out: os.Stderr})

// NewConfigLive is the constructor for *ConfigLive
func NewConfigLive(jetstreamContext nats.JetStreamContext) (*ConfigLive, error) {
	cl := &ConfigLive{
		jc:         jetstreamContext,
		updateChan: make(chan *nats.Msg),
	}

	err := cl.setupKVStore()
	if err != nil {
		return nil, err
	}

	err = cl.setupJetStreamStream()
	if err != nil {
		return nil, err
	}

	return cl, nil
}

type ConfigLive struct {
	jc         nats.JetStreamContext
	kv         nats.KeyValue
	updateChan chan *nats.Msg
}

func (cl *ConfigLive) setupKVStore() error {
	bucketName := config_internal.JetStreamStreamName

	kv, err := cl.jc.KeyValue(bucketName)
	if err == nil {
		cl.kv = kv
		return nil
	}

	kv, err = cl.jc.CreateKeyValue(&nats.KeyValueConfig{Bucket: bucketName})
	if err == nil {
		cl.kv = kv
		return nil
	}

	configLiveLogger.Error().Err(err).Msg("Failed to setup KV store")
	return err
}

func (cl *ConfigLive) setupJetStreamStream() error {
	stream, err := cl.jc.StreamInfo(config_internal.JetStreamStreamName)
	if err != nil {
		if err.Error() != "nats: stream not found" {
			return err
		}
	}
	if stream == nil {
		configLiveLogger.Info().Msg("Creating a JetStream stream")

		_, err = cl.jc.AddStream(&nats.StreamConfig{
			Name:     config_internal.JetStreamStreamName,
			Subjects: []string{config_internal.JetStreamStreamSubjects},
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// Publish a new config by passing it into JetStream with configKey identifier
func (cl *ConfigLive) Publish(configKey string, data []byte) error {
	_, err := cl.jc.Publish(configKey, data)
	if err != nil {
		configLiveLogger.Error().Err(err).Str("configKey", configKey).Msg("Failed to publish config")
	}

	return err
}

func (cl *ConfigLive) retrySubscribing(configKey string) *nats.Subscription {
	errLogger := configLiveLogger.Error().Str("configKey", configKey)

	sub, err := cl.jc.ChanSubscribe(configKey, cl.updateChan)
	n := 0
	for err != nil {
		if n > 20 {
			n = 0
		}

		// Log the error and then sleep before subscribing
		errLogger.Err(err).Msg("Failed to subscribe")
		time.Sleep(time.Duration(n*5) * time.Second)

		sub, err = cl.jc.ChanSubscribe(configKey, cl.updateChan)
		n += 1
	}

	return sub
}

// Run listens to config changes and update the storage
// TODO: Use the most consistent settings.
func (cl *ConfigLive) Run() {
	subscription := cl.retrySubscribing(config_internal.JetStreamStreamSubjects)
	defer subscription.Unsubscribe()

	configLiveLogger.Info().Msg("Subscribing to nats subjects")

	// Wait until we get a new message
	for {
		msg := <-cl.updateChan

		configKey := msg.Subject
		configBytes := msg.Data

		logger := configLiveLogger.With().Str("configKey", configKey).Logger()

		// Get existing config
		existingConfig, err := cl.kv.Get(configKey)
		if err != nil {
			logger.Error().Err(err).Msg("Failed to get existing config")
			return
		}

		// Don't do anything if new config is the same as existing config
		existingConfigBytes := existingConfig.Value()
		if bytes.Equal(existingConfigBytes, configBytes) {
			return
		}

		// Update config in kv store
		revision, err := cl.kv.Put(configKey, configBytes)
		if err != nil {
			logger.Error().Err(err).Msg("Failed to update config in KV store")
		} else {
			logger.Info().Int64("revision", int64(revision)).Msg("Updated config in KV store")
		}
	}
}

// GetConfigBytes returns config from the KV backend in bytes
func (cl *ConfigLive) GetConfigBytes(key string) ([]byte, error) {
	entry, err := cl.kv.Get(key)
	if err != nil {
		configLiveLogger.Error().Err(err).Str("configKey", key).Msg("Failed to get config from KV store")
		return nil, err
	}

	return entry.Value(), nil
}
