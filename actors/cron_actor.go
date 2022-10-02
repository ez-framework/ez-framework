package actors

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/nats-io/nats.go"

	"github.com/ez-framework/ez-framework/cron"
	"github.com/ez-framework/ez-framework/http_helpers"
)

// NewCronActor is the constructor for CronActors
func NewCronActor(actorConfig ActorConfig) (*CronActor, error) {
	name := "ez-cron"

	actor := &CronActor{
		Actor: Actor{
			actorConfig: actorConfig,
			streamName:  name,
			ConfigKV:    actorConfig.ConfigKV,
		},
		CronCollection: cron.NewCronCollection(actorConfig.JetStreamContext),
		IsLeader:       make(chan bool),
		IsFollower:     make(chan bool),
	}

	// CronActor cannot use nats.WorkQueuePolicy.
	// Config must be published to all subscribers because we don't know which instance is the leader.
	if actor.actorConfig.StreamConfig.Retention == nats.WorkQueuePolicy {
		actor.actorConfig.StreamConfig.Retention = nats.LimitsPolicy
	}

	err := actor.setupConstructor()
	if err != nil {
		return nil, err
	}

	actor.SetOnConfigUpdate(actor.configUpdateHandler)
	actor.SetOnConfigDelete(actor.configDeleteHandler)

	return actor, nil
}

type CronActor struct {
	Actor
	CronCollection *cron.CronCollection
	IsLeader       chan bool
	IsFollower     chan bool
}

// configUpdateHandler receives a config from jetstream and update the cron configuration
func (actor *CronActor) configUpdateHandler(ctx context.Context, msg *nats.Msg) {
	configBytes := msg.Data

	conf := cron.CronConfig{}

	err := json.Unmarshal(configBytes, &conf)
	if err != nil {
		actor.errorLogger.Err(err).Msg("failed to unmarshal config")
		return
	}

	// Update CronCollection's config in memory
	actor.CronCollection.Update(conf)

	// We are not running the new cron scheduler yet.
	// That job is handled by OnBecomingLeaderBlocking.
}

// configDeleteHandler listens to DELETE command and removes this particular cron scheduler
func (actor *CronActor) configDeleteHandler(ctx context.Context, msg *nats.Msg) {
	configBytes := msg.Data

	conf := cron.CronConfig{}

	err := json.Unmarshal(configBytes, &conf)
	if err != nil {
		actor.errorLogger.Err(err).Msg("failed to unmarshal config")
		return
	}

	actor.CronCollection.Delete(conf)

	// We still want to subscribe to jetstream to listen to more config changes
}

// OnBecomingLeaderBlocking turn on all cron schedulers.
func (actor *CronActor) OnBecomingLeaderBlocking(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case <-actor.IsLeader:
			actor.CronCollection.BecomesLeader()
		}
	}
}

// OnBecomingFollowerBlocking turn off all cron schedulers.
func (actor *CronActor) OnBecomingFollowerBlocking(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case <-actor.IsFollower:
			actor.CronCollection.BecomesFollower()
		}
	}
}

// OnBootLoadConfig loads cron config from KV store and notify the listener to setup cron schedulers.
func (actor *CronActor) OnBootLoadConfig() error {
	keys, err := actor.kv().Keys()
	if err != nil {
		return err
	}

	for _, configKey := range keys {
		if strings.HasPrefix(configKey, actor.streamName+".") {
			configBytes, err := actor.ConfigKV.GetConfigBytes(configKey)
			if err != nil {
				actor.errorLogger.Err(err).Msg("failed to get config JSON bytes")
				return err
			}

			err = actor.PublishConfig(actor.keyWithCommand(actor.streamName, "UPDATE"), configBytes)
			if err != nil {
				actor.errorLogger.Err(err).Msg("failed to publish")
			}
		}
	}

	return nil
}

// ServeHTTP supports updating and deleting object configuration via HTTP.
// Supported commands are POST, PUT, DELETE, and UNSUB
// HTTP GET should only be supported by the underlying object.
// Override this method if you want to do something custom.
func (actor *CronActor) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	configJSONBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http_helpers.RenderJSONError(actor.errorLogger, w, r, err, http.StatusInternalServerError)
		return
	}

	// Unpack to get the ID
	conf := cron.CronConfig{}

	err = json.Unmarshal(configJSONBytes, &conf)
	if err != nil {
		actor.errorLogger.Err(err).Msg("failed to unmarshal config")
		http_helpers.RenderJSONError(actor.errorLogger, w, r, err, http.StatusInternalServerError)
		return
	}

	command := actor.normalizeCommandFromHTTP(r)

	// Update KV store
	switch command {
	case "UPDATE":
		revision, err := actor.configPut(actor.streamName+"."+conf.ID, configJSONBytes)
		if err != nil {
			actor.errorLogger.Err(err).Msg("failed to update config in KV store")
			http_helpers.RenderJSONError(actor.errorLogger, w, r, err, http.StatusInternalServerError)
			return

		} else {
			actor.infoLogger.Int64("revision", int64(revision)).Msg("updated config in KV store")
		}

	case "DELETE":
		err = actor.configDelete(actor.streamName + "." + conf.ID)
		if err != nil {
			actor.errorLogger.Err(err).Msg("failed to delete config")
			http_helpers.RenderJSONError(actor.errorLogger, w, r, err, http.StatusInternalServerError)
			return
		}
	}

	// Push config to listeners
	err = actor.PublishConfig(actor.keyWithCommand(actor.streamName, command), configJSONBytes)
	if err != nil {
		http_helpers.RenderJSONError(actor.errorLogger, w, r, err, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write([]byte(`{"status":"success"}`))
}
