package actors

import (
	"bytes"
	"encoding/json"
	"net/http"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"

	"github.com/ez-framework/ez-framework/configkv"
	"github.com/ez-framework/ez-framework/http_helpers"
)

// NewConfigActor is the constructor for *ConfigActor
func NewConfigActor(jetstreamContext nats.JetStreamContext, confkv *configkv.ConfigKV) (*ConfigActor, error) {
	name := "ez-config"

	actor := &ConfigActor{
		Actor: Actor{
			jc:            jetstreamContext,
			jetstreamName: name,
			infoLogger:    log.Info().Str("stream.name", name),
			errorLogger:   log.Error().Str("stream.name", name),
			ConfigKV:      confkv,
		},
	}

	err := actor.setupJetStreamStream()
	if err != nil {
		return nil, err
	}

	return actor, nil
}

type ConfigActor struct {
	Actor
}

func (actor *ConfigActor) jetstreamSubscribeSubjects() string {
	return actor.jetstreamName + ".>"
}

// updateHandler will be executed inside Run.
func (actor *ConfigActor) updateHandler(configJSON map[string]interface{}) error {
	for configKey, value := range configJSON {
		configBytes, err := json.Marshal(value)
		if err != nil {
			actor.errorLogger.Err(err).Msg("failed to marshal config into JSON bytes")
			return err
		}

		// Get existing config
		existingConfig, err := actor.kv().Get(configKey)
		if err == nil {
			// Don't do anything if new config is the same as existing config
			existingConfigBytes := existingConfig.Value()
			if bytes.Equal(existingConfigBytes, configBytes) {
				return nil
			}
		}

		// Update config in kv store
		revision, err := actor.kv().Put(configKey, configBytes)
		if err != nil {
			actor.errorLogger.Err(err).Msg("failed to update config in KV store")
			return err

		} else {
			actor.infoLogger.Int64("revision", int64(revision)).Msg("updated config in KV store")
		}
	}

	return nil
}

// deleteHandler will be executed inside Run.
func (actor *ConfigActor) deleteHandler(configJSON map[string]interface{}) error {
	for configKey, _ := range configJSON {
		err := actor.kv().Delete(actor.keyWithoutCommand(configKey))
		if err != nil {
			actor.errorLogger.Err(err).Msg("failed to delete config in KV store")
			return err
		}
	}

	return nil
}

// Run listens to config changes and update the storage
func (actor *ConfigActor) Run() {
	actor.infoLogger.Caller().Msg("subscribing to nats subjects")

	actor.jc.QueueSubscribe(actor.jetstreamSubscribeSubjects(), "workers", func(msg *nats.Msg) {
		configJSONBytes := msg.Data
		configJSON := make(map[string]interface{})

		err := json.Unmarshal(configJSONBytes, &configJSON)
		if err != nil {
			actor.errorLogger.Err(err).
				Caller().
				Err(err).
				Msg("failed to unmarshal config inside Run()")
		}

		if actor.keyHasCommand(msg.Subject, "POST") || actor.keyHasCommand(msg.Subject, "PUT") {
			err := actor.updateHandler(configJSON)
			if err != nil {
				actor.errorLogger.Err(err).
					Caller().
					Err(err).
					Msg("failed to execute updateHandler inside Run()")
			}

		} else if actor.keyHasCommand(msg.Subject, "DELETE") {
			err := actor.deleteHandler(configJSON)
			if err != nil {
				actor.errorLogger.Err(err).
					Caller().
					Err(err).
					Msg("failed to execute deleteHandler inside Run()")
			}
		}
	})
}

// ServeHTTP supports updating and deleting via HTTP.
// Actor's HTTP handler always support only POST, PUT, and DELETE
// HTTP GET should only be supported by the underlying struct.
func (actor *ConfigActor) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	content := make(map[string]interface{})

	err := json.NewDecoder(r.Body).Decode(&content)
	if err != nil {
		http_helpers.RenderJSONError(actor.errorLogger, w, r, err, http.StatusInternalServerError)
		return
	}

	originalJSONBytes, err := json.Marshal(content)
	if err != nil {
		http_helpers.RenderJSONError(actor.errorLogger, w, r, err, http.StatusInternalServerError)
		return
	}

	// 1. Publish to top level, which is actor.jetstreamName
	// Example: Publish(ez-configlive.command:POST)
	//          Payload: {"raft-node": {"LogPath":"./.data/graft.log","ClusterName":"cluster","ClusterSize":3,"NatsAddr":"nats://127.0.0.1:4222"}}
	// The config will be saved in ConfigKV store.
	err = actor.Publish(actor.keyWithCommand(actor.jetstreamName, r.Method), originalJSONBytes)
	if err != nil {
		http_helpers.RenderJSONError(actor.errorLogger, w, r, err, http.StatusInternalServerError)
		return
	}

	for key, value := range content {
		valueJSONBytes, err := json.Marshal(value)
		if err != nil {
			http_helpers.RenderJSONError(actor.errorLogger, w, r, err, http.StatusInternalServerError)
			return
		}

		// 2. Publish to the watchers
		println(key)
		println(actor.keyWithCommand(key, r.Method))
		println(string(valueJSONBytes))

		err = actor.Publish(actor.keyWithCommand(key, r.Method), valueJSONBytes)
		if err != nil {
			http_helpers.RenderJSONError(actor.errorLogger, w, r, err, http.StatusInternalServerError)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write([]byte(`{"status":"success"}`))
}
