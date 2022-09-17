package actors

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/go-chi/render"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/ez-framework/ez-framework/config_internal"
	"github.com/ez-framework/ez-framework/configkv"
)

var configActorLogger = log.With().
	Str("streamName", config_internal.JetStreamStreamName).
	Str("streamSubjects", config_internal.JetStreamStreamSubjects).
	Str("KVBucket", config_internal.KVBucketName).
	Logger().Output(zerolog.ConsoleWriter{Out: os.Stderr})

// NewConfigActor is the constructor for *ConfigActor
func NewConfigActor(jetstreamContext nats.JetStreamContext) (*ConfigActor, error) {
	configactor := &ConfigActor{
		jc:         jetstreamContext,
		updateChan: make(chan *nats.Msg),
	}

	err := configactor.setupConfigKVStore()
	if err != nil {
		return nil, err
	}

	err = configactor.setupJetStreamStream()
	if err != nil {
		return nil, err
	}

	return configactor, nil
}

type ConfigActor struct {
	jc         nats.JetStreamContext
	ConfigKV   *configkv.ConfigKV
	updateChan chan *nats.Msg
}

func (configactor *ConfigActor) setupConfigKVStore() error {
	confkv, err := configkv.NewConfigKV(configactor.jc)
	if err != nil {
		configActorLogger.Error().Err(err).Msg("Failed to setup KV store")
		return err
	}
	configactor.ConfigKV = confkv
	return nil
}

func (configactor *ConfigActor) kv() nats.KeyValue {
	return configactor.ConfigKV.KV
}

func (configactor *ConfigActor) setupJetStreamStream() error {
	stream, err := configactor.jc.StreamInfo(config_internal.JetStreamStreamName)
	if err != nil {
		if err.Error() != "nats: stream not found" {
			return err
		}
	}
	if stream == nil {
		configActorLogger.Info().Msg("Creating a JetStream stream")

		_, err = configactor.jc.AddStream(&nats.StreamConfig{
			Name:     config_internal.JetStreamStreamName,
			Subjects: []string{config_internal.JetStreamStreamSubjects},
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (configactor *ConfigActor) renderJSONError(w http.ResponseWriter, r *http.Request, err error, code int) {
	configActorLogger.Error().Err(err).Msg("Failed to respond HTTP request")

	content := make(map[string]string)
	content["error"] = err.Error()
	w.WriteHeader(code)
	render.JSON(w, r, content)
}

// kvKeyWithoutCommand strips the command which is appended at the end
func (configactor *ConfigActor) kvKeyWithoutCommand(key string) string {
	keyEndIndex := strings.Index(key, ".command:")
	return key[0:keyEndIndex]
}

func (configactor *ConfigActor) kvKeyWithCommand(key, command string) string {
	return key + ".command:" + command
}

func (configactor *ConfigActor) kvKeyHasCommand(key, command string) bool {
	return strings.HasSuffix(key, ".command:"+command)
}

// ServeHTTP supports publishing via HTTP
func (configactor *ConfigActor) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	content := make(map[string]interface{})

	err := json.NewDecoder(r.Body).Decode(&content)
	if err != nil {
		configactor.renderJSONError(w, r, err, http.StatusInternalServerError)
		return
	}

	for key, value := range content {
		if !strings.HasPrefix(key, config_internal.JetStreamStreamName+".") {
			key = config_internal.JetStreamStreamName + "." + key
		}

		valueJSONBytes, err := json.Marshal(value)
		if err != nil {
			configactor.renderJSONError(w, r, err, http.StatusInternalServerError)
			return
		}

		publishKey := configactor.kvKeyWithCommand(key, "update")

		if r.Method == "DELETE" {
			publishKey = configactor.kvKeyWithCommand(key, "delete")
		}

		err = configactor.Publish(publishKey, valueJSONBytes)
		if err != nil {
			configactor.renderJSONError(w, r, err, http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Write([]byte(`{"status":"success"}`))
	}
}

// Publish a new config by passing it into JetStream with configKey identifier
func (configactor *ConfigActor) Publish(configKey string, data []byte) error {
	_, err := configactor.jc.Publish(configKey, data)
	if err != nil {
		configActorLogger.Error().Err(err).Str("configKey", configKey).Msg("Failed to publish config")
	}

	return err
}

func (configactor *ConfigActor) retrySubscribing(configKey string) *nats.Subscription {
	errLogger := configActorLogger.Error().Str("configKey", configKey)

	sub, err := configactor.jc.ChanSubscribe(configKey, configactor.updateChan)
	n := 0
	for err != nil {
		if n > 20 {
			n = 0
		}

		// Log the error and then sleep before subscribing
		errLogger.Err(err).Msg("Failed to subscribe")
		time.Sleep(time.Duration(n*5) * time.Second)

		sub, err = configactor.jc.ChanSubscribe(configKey, configactor.updateChan)
		n += 1
	}

	return sub
}

// Run listens to config changes and update the storage
// TODO: Use the most consistent settings.
func (configactor *ConfigActor) Run() {
	subscription := configactor.retrySubscribing(config_internal.JetStreamStreamSubjects)
	defer subscription.Unsubscribe()

	configActorLogger.Info().Msg("Subscribing to nats subjects")

	// Wait until we get a new message
	for {
		msg := <-configactor.updateChan

		configKey := msg.Subject
		configBytes := msg.Data

		logger := configActorLogger.With().Str("configKey", configKey).Logger()

		if configactor.kvKeyHasCommand(configKey, "update") {
			// Get existing config
			existingConfig, err := configactor.kv().Get(configactor.kvKeyWithoutCommand(configKey))
			if err == nil {
				// Don't do anything if new config is the same as existing config
				existingConfigBytes := existingConfig.Value()
				if bytes.Equal(existingConfigBytes, configBytes) {
					return
				}
			}

			// Update config in kv store
			revision, err := configactor.kv().Put(configactor.kvKeyWithoutCommand(configKey), configBytes)
			if err != nil {
				logger.Error().Err(err).Msg("Failed to update config in KV store")
			} else {
				logger.Info().Int64("revision", int64(revision)).Msg("Updated config in KV store")
			}

		} else if configactor.kvKeyHasCommand(configKey, "delete") {
			err := configactor.kv().Delete(configactor.kvKeyWithoutCommand(configKey))
			if err != nil {
				logger.Error().Err(err).Msg("Failed to delete config in KV store")
			}
		}
	}
}
