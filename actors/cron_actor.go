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
			jc:          actorConfig.JetStreamContext,
			streamName:  name,
			infoLogger:  log.Info().Str("stream.name", name),
			errorLogger: log.Error().Str("stream.name", name),
			ConfigKV:    actorConfig.ConfigKV,
		},
		CronCollection: cron.NewCronCollection(actorConfig.JetStreamContext),
	}

	err := actor.setupStream(actorConfig.StreamConfig)
	if err != nil {
		return nil, err
	}

	return actor, nil
}

type CronActor struct {
	Actor
	CronCollection *cron.CronCollection
	subscription   *nats.Subscription
}

// subscribeSubjects
func (actor *CronActor) subscribeSubjects() string {
	return actor.streamName + ".>"
}

// Stop
func (actor *CronActor) Stop() error {
	err := actor.subscription.Unsubscribe()
	if err == nil {
		actor.subscription = nil
		actor.CronCollection.Stop()
	}
	return err
}

// RunOnConfigUpdate listens to config changes and update the CronCollection
func (actor *CronActor) RunOnConfigUpdate() {
	actor.infoLogger.
		Caller().
		Str("subjects.subscribe", actor.subscribeSubjects()).
		Msg("subscribing to nats subjects")

	// We subscribe as a queue because there's only 1 worker needed to listen to config changes
	sub, err := actor.jc.QueueSubscribe(actor.subscribeSubjects(), "workers", func(msg *nats.Msg) {
		configBytes := msg.Data

		conf := cron.CronConfig{}

		err := json.Unmarshal(configBytes, &conf)
		if err != nil {
			actor.errorLogger.Err(err).Msg("failed to unmarshal config")
			return
		}

		if actor.keyHasCommand(msg.Subject, "POST") || actor.keyHasCommand(msg.Subject, "PUT") {
			actor.CronCollection.Update(conf)

			_, err := actor.kv().Put(actor.streamName+"."+conf.ID, configBytes)
			if err != nil {
				actor.errorLogger.Err(err).Msg("failed to save config")
				return
			}

			// TODO: We need to check of this instance is the leader
			actor.CronCollection.Run(conf)

		} else if actor.keyHasCommand(msg.Subject, "DELETE") {
			actor.CronCollection.Delete(conf)

			err := actor.kv().Delete(actor.streamName + "." + conf.ID)
			if err != nil {
				actor.errorLogger.Err(err).Msg("failed to delete config")
				return
			}
		}
	})

	if err == nil {
		actor.subscription = sub
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