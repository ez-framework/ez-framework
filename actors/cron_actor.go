package actors

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"

	"github.com/ez-framework/ez-framework/cron"
	"github.com/ez-framework/ez-framework/http_helpers"
)

func NewCronActor(globalConfig GlobalConfig) (*CronActor, error) {
	name := "ez-cron"

	actor := &CronActor{
		Actor: Actor{
			globalConfig:  globalConfig,
			jc:            globalConfig.JetStreamContext,
			jetstreamName: name,
			infoLogger:    log.Info().Str("stream.name", name),
			errorLogger:   log.Error().Str("stream.name", name),
			ConfigKV:      globalConfig.ConfigKV,
		},
		CronCollection: cron.NewCronCollection(globalConfig.JetStreamContext),
	}

	err := actor.setupJetStreamStream(&nats.StreamConfig{
		MaxAge: 1 * time.Minute,
	})
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

// jetstreamSubscribeSubjects
func (actor *CronActor) jetstreamSubscribeSubjects() string {
	return actor.jetstreamName + ".>"
}

// Stop
func (actor *CronActor) Stop() error {
	err := actor.subscription.Unsubscribe()
	if err == nil {
		actor.subscription = nil
	}
	return err
}

// Run
func (actor *CronActor) Run() {
	actor.infoLogger.
		Caller().
		Str("subjects.subscribe", actor.jetstreamSubscribeSubjects()).
		Msg("subscribing to nats subjects")

	sub, err := actor.jc.Subscribe(actor.jetstreamSubscribeSubjects(), func(msg *nats.Msg) {
		configBytes := msg.Data

		conf := cron.CronConfig{}

		err := json.Unmarshal(configBytes, &conf)
		if err != nil {
			actor.errorLogger.Err(err).Msg("failed to unmarshal config")
			return
		}

		if actor.keyHasCommand(msg.Subject, "POST") || actor.keyHasCommand(msg.Subject, "PUT") {
			actor.CronCollection.Update(conf)

			_, err := actor.kv().Put(actor.jetstreamName+"."+conf.ID, configBytes)
			if err != nil {
				actor.errorLogger.Err(err).Msg("failed to save config")
				return
			}

			// TODO: We need to check of this instance is the leader
			actor.CronCollection.Run(conf)

		} else if actor.keyHasCommand(msg.Subject, "DELETE") {
			actor.CronCollection.Delete(conf)

			err := actor.kv().Delete(actor.jetstreamName + "." + conf.ID)
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

func (actor *CronActor) OnBootLoad() error {
	keys, err := actor.kv().Keys()
	if err != nil {
		return err
	}

	for _, configKey := range keys {
		if strings.HasPrefix(configKey, actor.jetstreamName+".") {
			configBytes, err := actor.ConfigKV.GetConfigBytes(configKey)
			if err != nil {
				actor.errorLogger.Err(err).Msg("failed to get config JSON bytes")
				return err
			}

			err = actor.Publish(actor.keyWithCommand(actor.jetstreamName, "POST"), configBytes)
			if err != nil {
				actor.errorLogger.Err(err).Msg("failed to publish")
			}
		}
	}

	return nil
}

// ServeHTTP supports updating and deleting via HTTP.
// Actor's HTTP handler always support only POST, PUT, and DELETE
// HTTP GET should only be supported by the underlying struct.
func (actor *CronActor) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	configJSONBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http_helpers.RenderJSONError(actor.errorLogger, w, r, err, http.StatusInternalServerError)
		return
	}

	err = actor.Publish(actor.keyWithCommand(actor.jetstreamName, r.Method), configJSONBytes)
	if err != nil {
		http_helpers.RenderJSONError(actor.errorLogger, w, r, err, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write([]byte(`{"status":"success"}`))
}
