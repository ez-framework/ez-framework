package cron

import (
	"os"
	"sync"
	"time"

	"github.com/go-co-op/gocron"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/diode"
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
	cc := &CronCollection{
		jc:             jc,
		c:              make(map[string]*gocron.Scheduler),
		confCollection: make(map[string]CronConfig),
		mtx:            sync.RWMutex{},
	}

	outWriter := diode.NewWriter(os.Stdout, 1000, 0, nil)
	errWriter := diode.NewWriter(os.Stderr, 1000, 0, nil)

	cc.infoLogger = zerolog.New(zerolog.ConsoleWriter{Out: outWriter, TimeFormat: time.RFC3339}).With().Timestamp().Logger()
	cc.errorLogger = zerolog.New(zerolog.ConsoleWriter{Out: errWriter, TimeFormat: time.RFC3339}).With().Timestamp().Logger()
	cc.debugLogger = zerolog.New(zerolog.ConsoleWriter{Out: errWriter, TimeFormat: time.RFC3339}).With().Timestamp().Logger()

	return cc
}

// CronCollection holds a collection of cron schedulers.
type CronCollection struct {
	isLeader       bool
	isFollower     bool
	c              map[string]*gocron.Scheduler
	confCollection map[string]CronConfig
	jc             nats.JetStreamContext
	mtx            sync.RWMutex
	infoLogger     zerolog.Logger
	errorLogger    zerolog.Logger
	debugLogger    zerolog.Logger
}

func (collection *CronCollection) workerQueueStreamName(config CronConfig) string {
	return "ez-worker-" + config.WorkerQueue
}

func (collection *CronCollection) log(lvl zerolog.Level) *zerolog.Event {
	switch lvl {
	case zerolog.ErrorLevel:
		return collection.errorLogger.Error()
	case zerolog.DebugLevel:
		return collection.debugLogger.Debug()
	default:
		return collection.infoLogger.Info()
	}
}

// Update receives config update from jetstream and configure the cron scheduler
func (collection *CronCollection) Update(config CronConfig) {
	collection.Delete(config)

	location, err := time.LoadLocation(config.Timezone)
	if err != nil {
		collection.log(zerolog.ErrorLevel).Caller().Err(err).Msg("unable to detect timezone, defaulting to UTC")
		location = time.UTC
	}

	scheduler := gocron.NewScheduler(location)
	scheduler = scheduler.Cron(config.Schedule)

	collection.mtx.Lock()
	collection.c[config.ID] = scheduler
	collection.confCollection[config.ID] = config
	collection.mtx.Unlock()

	scheduler.Do(func() {
		_, err := collection.jc.Publish(collection.workerQueueStreamName(config)+".command:UPDATE", []byte(`{}`))
		if err != nil {
			collection.log(zerolog.ErrorLevel).Caller().Err(err).
				Str("cron.id", config.ID).
				Str("cron.schedule", config.Schedule).
				Str("cron.timezone", config.Timezone).
				Str("cron.worker-queue", collection.workerQueueStreamName(config)).
				Msg("failed to publish to JetStream to trigger work on worker queue")
		}
	})
}

// BecomesLeader keep track of leader/follower state
func (collection *CronCollection) BecomesLeader() {
	collection.mtx.Lock()
	collection.isLeader = true
	collection.isFollower = false
	collection.mtx.Unlock()

	collection.StartAll()
}

// BecomesFollower keep track of leader/follower state
func (collection *CronCollection) BecomesFollower() {
	collection.mtx.Lock()
	collection.isLeader = true
	collection.isFollower = false
	collection.mtx.Unlock()

	collection.StopAll()
}

// Delete a cron scheduler configuration
func (collection *CronCollection) Delete(config CronConfig) {
	collection.mtx.Lock()
	defer collection.mtx.Unlock()

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
