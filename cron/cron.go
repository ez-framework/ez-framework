package cron

import (
	"time"

	"github.com/go-co-op/gocron"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// CronConfig is the setting for cronjob
type CronConfig struct {
	ID          string
	Schedule    string
	Timezone    string
	WorkerQueue string
}

// NewCronCollection is the constructor for CronCollection
func NewCronCollection(jc nats.JetStreamContext) *CronCollection {
	return &CronCollection{
		jc:             jc,
		c:              make(map[string]*gocron.Scheduler),
		confCollection: make(map[string]CronConfig),
		errorLogger:    log.Error(),
	}
}

// CronCollection holds a collection of cron schedulers.
type CronCollection struct {
	isLeader       bool
	isFollower     bool
	c              map[string]*gocron.Scheduler
	confCollection map[string]CronConfig
	jc             nats.JetStreamContext
	errorLogger    *zerolog.Event
}

func (collection *CronCollection) workerQueueStreamName(config CronConfig) string {
	return "ez-worker-" + config.WorkerQueue
}

// Update receives config update from jetstream and configure the cron scheduler
func (collection *CronCollection) Update(config CronConfig) {
	collection.Delete(config)

	scheduler := gocron.NewScheduler(time.UTC) // TODO: elegantly assign timezone based on config
	scheduler = scheduler.Cron(config.Schedule)
	collection.c[config.ID] = scheduler
	collection.confCollection[config.ID] = config

	scheduler.Do(func() {
		_, err := collection.jc.Publish(collection.workerQueueStreamName(config)+".command:POST", []byte(`{}`))
		if err != nil {
			collection.errorLogger.Err(err).
				Str("cron.id", config.ID).
				Str("cron.schedule", config.Schedule).
				Str("cron.timezone", config.Timezone).
				Str("cron.worker-queue", collection.workerQueueStreamName(config)).
				Msg("failed to publish to JetStream")
		}
	})
}

// BecomesLeader keep track of leader/follower state
func (collection *CronCollection) BecomesLeader() {
	collection.isLeader = true
	collection.isFollower = false

	collection.StartAll()
}

// BecomesFollower keep track of leader/follower state
func (collection *CronCollection) BecomesFollower() {
	collection.isLeader = true
	collection.isFollower = false

	collection.StopAll()
}

// Delete a cron scheduler configuration
func (collection *CronCollection) Delete(config CronConfig) {
	existing, ok := collection.c[config.ID]
	if ok {
		existing.Stop()
		delete(collection.c, config.ID)
		delete(collection.confCollection, config.ID)
	}
}

// StopAll cron schedulers.
func (collection *CronCollection) StopAll() {
	for _, existing := range collection.c {
		existing.Stop()
	}
}

// StartAll cron schedulers.
func (collection *CronCollection) StartAll() {
	for _, existing := range collection.c {
		existing.StartAsync()
	}
}

// AllConfigs shows all cron configurations.
func (collection *CronCollection) AllConfigs() map[string]CronConfig {
	return collection.confCollection
}
