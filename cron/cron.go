package cron

import (
	"time"

	"github.com/go-co-op/gocron"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type CronConfig struct {
	ID          string
	Schedule    string
	Timezone    string
	WorkerQueue string
}

func NewCronCollection(jc nats.JetStreamContext) *CronCollection {
	return &CronCollection{
		jc:             jc,
		c:              make(map[string]*gocron.Scheduler),
		confCollection: make(map[string]CronConfig),
		errorLogger:    log.Error(),
	}
}

type CronCollection struct {
	c              map[string]*gocron.Scheduler
	confCollection map[string]CronConfig
	jc             nats.JetStreamContext
	errorLogger    *zerolog.Event
}

func (collection *CronCollection) Update(config CronConfig) {
	existing, ok := collection.c[config.ID]
	if ok {
		existing.Stop()
	}

	scheduler := gocron.NewScheduler(time.UTC) // TODO: elegantly assign timezone based on config
	scheduler = scheduler.Cron(config.Schedule)
	collection.c[config.ID] = scheduler
	collection.confCollection[config.ID] = config

	scheduler.Do(func() {
		_, err := collection.jc.Publish(config.WorkerQueue+".command:POST", []byte(`{}`))
		if err != nil {
			collection.errorLogger.Err(err).
				Str("cron.id", config.ID).
				Str("cron.schedule", config.Schedule).
				Str("cron.timezone", config.Timezone).
				Str("cron.worker-queue", config.WorkerQueue).
				Msg("failed to publish to JetStream")
		}
	})
}

func (collection *CronCollection) Run(config CronConfig) {
	collection.c[config.ID].StartAsync()
}

func (collection *CronCollection) Delete(config CronConfig) {
	existing, ok := collection.c[config.ID]
	if ok {
		existing.Stop()
		delete(collection.c, config.ID)
		delete(collection.confCollection, config.ID)
	}
}

func (collection *CronCollection) AllConfigs() map[string]CronConfig {
	return collection.confCollection
}
