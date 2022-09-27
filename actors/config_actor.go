package actors

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/nats-io/nats.go"
)

// NewConfigActor is the constructor for *ConfigActor
func NewConfigActor(actorConfig ActorConfig) (*ConfigActor, error) {
	name := "ez-configkv"

	actor := &ConfigActor{
		Actor: Actor{
			actorConfig: actorConfig,
			streamName:  name,
			ConfigKV:    actorConfig.ConfigKV,
		},
		Downstreams: map[string][]string{
			"all":     {"ez-config-ws"},
			"ez-raft": {"ez-raft"},
		},
	}

	err := actor.setupConstructor()
	if err != nil {
		return nil, err
	}

	actor.SetSubscribers("POST", actor.updateHandler)
	actor.SetSubscribers("PUT", actor.updateHandler)
	actor.SetSubscribers("DELETE", actor.deleteHandler)

	return actor, nil
}

// ConfigActor listens to changes from JetStream and performs KV operations
type ConfigActor struct {
	Actor
	Downstreams map[string][]string
}

// publishToDownstreams sends configJSON to the predefined downstreams.
// The downstreams are defined in actor.Downstreams.
func (actor *ConfigActor) publishToDownstreams(configJSON map[string]interface{}, command string) error {
	configBytes, err := json.Marshal(configJSON)
	if err != nil {
		actor.errorLogger.Err(err).Msg("failed to marshal config JSON")
		return err
	}

	// Publish the entire config to all downstreams tagged with "all"
	for _, downstream := range actor.Downstreams["all"] {
		err = actor.Publish(actor.keyWithCommand(downstream, command), configBytes)
		if err != nil {
			actor.errorLogger.Err(err).Str("downstream", downstream).
				Msg("failed to push config to downstream subscribers")
		}
	}

	// Publish specific part of a config to downstreams tagged with the same name as the config.
	for configKey, config := range configJSON {
		for downstreamKey, downstreams := range actor.Downstreams {
			if configKey == downstreamKey {
				configValueJSONBytes, err := json.Marshal(config)
				if err != nil {
					actor.errorLogger.Err(err).Str("downstreamKey", downstreamKey).
						Msg("failed to marshal config for downstream subscribers")
					continue
				}

				for _, downstream := range downstreams {
					err = actor.Publish(actor.keyWithCommand(downstream, command), configValueJSONBytes)
					if err != nil {
						actor.errorLogger.Err(err).Str("downstreamKey", downstreamKey).
							Msg("failed to push config to downstream subscribers")
						continue
					}
				}
			}
		}
	}

	return nil
}

// updateHandler will be executed inside Run.
// It responds to POST and PUT commands.
func (actor *ConfigActor) updateHandler(ctx context.Context, msg *nats.Msg) {
	configJSONBytes := msg.Data
	configJSON := make(map[string]interface{})

	err := json.Unmarshal(configJSONBytes, &configJSON)
	if err != nil {
		actor.errorLogger.Err(err).
			Msg("failed to unmarshal config inside RunSubscribersBlocking()")
	}

	// Push config to downstream subscribers.
	err = actor.publishToDownstreams(configJSON, "POST")
	if err != nil {
		actor.errorLogger.Err(err).
			Msg("failed to published to downstreams")
	}

	// ---------------------------------------------------------------------------
	// For every config, save them in the KV store.
	for configKey, value := range configJSON {
		configBytes, err := json.Marshal(value)
		if err != nil {
			actor.errorLogger.Err(err).Msg("failed to marshal config into JSON bytes")
			return
		}

		// Get existing config
		existingConfig, err := actor.kv().Get(configKey)
		if err == nil {
			// Don't do anything if new config is the same as existing config
			existingConfigBytes := existingConfig.Value()
			if bytes.Equal(existingConfigBytes, configBytes) {
				return
			}
		}

		// Put config in kv store
		revision, err := actor.kv().Put(configKey, configBytes)
		if err != nil {
			actor.errorLogger.Err(err).Msg("failed to update config in KV store")
			return

		} else {
			actor.infoLogger.Int64("revision", int64(revision)).Msg("updated config in KV store")
		}
	}
}

// deleteHandler listens to DELETE command and do something
func (actor *ConfigActor) deleteHandler(ctx context.Context, msg *nats.Msg) {
	configJSONBytes := msg.Data
	configJSON := make(map[string]interface{})

	err := json.Unmarshal(configJSONBytes, &configJSON)
	if err != nil {
		actor.errorLogger.Err(err).
			Msg("failed to unmarshal config inside RunSubscribersBlocking()")
	}

	// Push config to downstream subscribers.
	err = actor.publishToDownstreams(configJSON, "DELETE")
	if err != nil {
		actor.errorLogger.Err(err).Msg("failed to publish downstream")
		return
	}

	// ---------------------------------------------------------------------------
	// Delete the config based on their keys
	for configKey := range configJSON {
		err := actor.kv().Delete(actor.keyWithoutCommand(configKey))
		if err != nil {
			actor.errorLogger.Err(err).Msg("failed to delete config in KV store")
			return
		}
	}
}
