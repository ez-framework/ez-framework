package actors

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"

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
			infoLogger:  log.Info().Str("stream.name", name),
			errorLogger: log.Error().Str("stream.name", name),
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

	err := actor.setupStream()
	if err != nil {
		return nil, err
	}

	actor.SetPOSTSubscriber(actor.updateHandler)
	actor.SetPUTSubscriber(actor.updateHandler)
	actor.SetDELETESubscriber(actor.deleteHandler)

	return actor, nil
}

type CronActor struct {
	Actor
	CronCollection *cron.CronCollection
	IsLeader       chan bool
	IsFollower     chan bool
}

// updateHandler receives a config from jetstream and update the cron configuration
func (actor *CronActor) updateHandler(msg *nats.Msg) {
	configBytes := msg.Data

	conf := cron.CronConfig{}

	err := json.Unmarshal(configBytes, &conf)
	if err != nil {
		actor.errorLogger.Err(err).Msg("failed to unmarshal config")
		return
	}

	// Save the config in KV store first before updating CronCollection.
	_, err = actor.kv().Put(actor.streamName+"."+conf.ID, configBytes)
	if err != nil {
		actor.errorLogger.Err(err).Msg("failed to save config")
		return
	}

	// Update CronCollection's config in memory
	actor.CronCollection.Update(conf)

	// We are not running the new cron scheduler yet.
	// That job is handled by OnBecomingLeaderSync.
}

// deleteHandler listens to DELETE command and removes this particular cron scheduler
func (actor *CronActor) deleteHandler(msg *nats.Msg) {
	configBytes := msg.Data

	conf := cron.CronConfig{}

	err := json.Unmarshal(configBytes, &conf)
	if err != nil {
		actor.errorLogger.Err(err).Msg("failed to unmarshal config")
		return
	}

	err = actor.kv().Delete(actor.streamName + "." + conf.ID)
	if err != nil {
		actor.errorLogger.Err(err).Msg("failed to delete config")
		return
	}

	actor.CronCollection.Delete(conf)

	// We still want to subscribe to jetstream to listen to more config changes
}

// Unsubscribe from stream
func (actor *CronActor) Unsubscribe() (err error) {
	if actor.subscription != nil {
		err = actor.subscription.Unsubscribe()
	}

	return err
}

// OnBecomingLeaderSync turn on all cron schedulers.
func (actor *CronActor) OnBecomingLeaderSync() {
	for {
		<-actor.IsLeader
		actor.CronCollection.BecomesLeader()
	}
}

// OnBecomingFollowerSync turn off all cron schedulers.
func (actor *CronActor) OnBecomingFollowerSync() {
	for {
		<-actor.IsFollower
		actor.CronCollection.BecomesFollower()
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

			err = actor.Publish(actor.keyWithCommand(actor.streamName, "POST"), configBytes)
			if err != nil {
				actor.errorLogger.Err(err).Msg("failed to publish")
			}
		}
	}

	return nil
}

// ServeHTTP supports updating and deleting cron via HTTP.
// Actor's HTTP handler always support only POST, PUT, and DELETE
// HTTP GET should only be supported by the underlying struct.
func (actor *CronActor) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	configJSONBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http_helpers.RenderJSONError(actor.errorLogger, w, r, err, http.StatusInternalServerError)
		return
	}

	err = actor.Publish(actor.keyWithCommand(actor.streamName, r.Method), configJSONBytes)
	if err != nil {
		http_helpers.RenderJSONError(actor.errorLogger, w, r, err, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write([]byte(`{"status":"success"}`))
}
