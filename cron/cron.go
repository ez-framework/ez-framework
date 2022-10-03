package cron

import (
	"encoding/json"
	"os"
	"sync"
	"time"

	"github.com/go-co-op/gocron"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/diode"

	"github.com/ez-framework/ez-framework/configkv"
)

// CronConfig is the setting for cronjob
type CronConfig struct {
	ID          string
	Schedule    string
	Timezone    string
	WorkerQueue string
}

// CronStatus is used to display the current status of cronjob
type CronStatus struct {
	ID          string
	Schedule    string
	Timezone    string
	WorkerQueue string
	IsRunning   bool
	LastRun     time.Time
	NextRun     time.Time
	RunCount    int
}

// CronCollectionConfig
type CronCollectionConfig struct {
	// JetStreamContext
	JetStreamContext nats.JetStreamContext

	// ConfigKV
	ConfigKV *configkv.ConfigKV
}

// NewCronCollection is the constructor for CronCollection
func NewCronCollection(ccc CronCollectionConfig) *CronCollection {
	cc := &CronCollection{
		jsc:              ccc.JetStreamContext,
		schedulers:       make(map[string]*gocron.Scheduler),
		schedulerConfigs: make(map[string]CronConfig),
		jobs:             make(map[string]*gocron.Job),
		mtx:              sync.RWMutex{},
		configKV:         ccc.ConfigKV,
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
	isLeader         bool
	isFollower       bool
	schedulers       map[string]*gocron.Scheduler
	schedulerConfigs map[string]CronConfig
	jobs             map[string]*gocron.Job
	jsc              nats.JetStreamContext
	configKV         *configkv.ConfigKV
	mtx              sync.RWMutex
	infoLogger       zerolog.Logger
	errorLogger      zerolog.Logger
	debugLogger      zerolog.Logger
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

// saveStatus
func (collection *CronCollection) saveStatus(configID string, statusBytes []byte) error {
	_, err := collection.configKV.Put("ez-cron-status."+configID, statusBytes)
	return err
}

// getStatus
func (collection *CronCollection) getStatus(configID string) (CronStatus, error) {
	status := CronStatus{}

	statusBytes, err := collection.configKV.GetConfigBytes("ez-cron-status." + configID)
	if err != nil {
		return status, err
	}

	err = json.Unmarshal(statusBytes, &status)
	if err != nil {
		return status, err
	}

	return status, nil
}

// saveCronjobInfo saves cronjob information into the database
func (collection *CronCollection) saveCronjobInfo(config CronConfig, job *gocron.Job) {
	status := CronStatus{
		ID:          config.ID,
		Schedule:    config.Schedule,
		Timezone:    config.Timezone,
		WorkerQueue: config.WorkerQueue,
	}

	status.IsRunning = job.IsRunning()
	status.LastRun = job.LastRun()
	status.NextRun = job.NextRun()
	status.RunCount = job.RunCount()

	statusBytes, err := json.Marshal(status)
	if err != nil {
		collection.log(zerolog.ErrorLevel).Caller().Err(err).
			Str("cron.id", config.ID).
			Str("cron.schedule", config.Schedule).
			Str("cron.timezone", config.Timezone).
			Str("cron.worker-queue", collection.workerQueueStreamName(config)).
			Msg("failed to marshal job status into the database")
	} else {
		err := collection.saveStatus(config.ID, statusBytes)
		collection.log(zerolog.ErrorLevel).Caller().Err(err).
			Str("cron.id", config.ID).
			Str("cron.schedule", config.Schedule).
			Str("cron.timezone", config.Timezone).
			Str("cron.worker-queue", collection.workerQueueStreamName(config)).
			Msg("failed to save job status into the database")
	}
}

// Update receives config update from jetstream and configure the cron scheduler
func (collection *CronCollection) Update(config CronConfig) {
	collection.Delete(config)

	location, err := time.LoadLocation(config.Timezone)
	if err != nil {
		collection.log(zerolog.ErrorLevel).Caller().Err(err).
			Bool("harmless", true).
			Msg("unable to detect timezone, defaulting to UTC")

		location = time.UTC
	}

	scheduler := gocron.NewScheduler(location)
	scheduler = scheduler.Cron(config.Schedule)

	collection.mtx.Lock()
	collection.schedulers[config.ID] = scheduler
	collection.schedulerConfigs[config.ID] = config
	collection.mtx.Unlock()

	job, err := scheduler.Do(func() {
		collection.log(zerolog.DebugLevel).
			Str("cron.id", config.ID).
			Str("cron.schedule", config.Schedule).
			Str("cron.timezone", config.Timezone).
			Str("cron.worker-queue", collection.workerQueueStreamName(config)).
			Msg("about to publish to JetStream to trigger work on worker queue")

		_, err := collection.jsc.Publish(collection.workerQueueStreamName(config)+".command:UPDATE", []byte(`{}`))
		if err != nil {
			collection.log(zerolog.ErrorLevel).Caller().Err(err).
				Str("cron.id", config.ID).
				Str("cron.schedule", config.Schedule).
				Str("cron.timezone", config.Timezone).
				Str("cron.worker-queue", collection.workerQueueStreamName(config)).
				Msg("failed to publish to JetStream to trigger work on worker queue")
		}
	})
	if err != nil {
		collection.log(zerolog.ErrorLevel).Caller().Err(err).
			Str("cron.id", config.ID).
			Str("cron.schedule", config.Schedule).
			Str("cron.timezone", config.Timezone).
			Str("cron.worker-queue", collection.workerQueueStreamName(config)).
			Msg("failed to schedule a cron function")
	}

	// Configure after execution hook.
	// We are saving the cronjob metadata into the database here.
	job.SetEventListeners(func() {}, func() {
		collection.saveCronjobInfo(config, job)
	})

	collection.mtx.Lock()
	collection.jobs[config.ID] = job
	collection.mtx.Unlock()
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
	existing, ok := collection.schedulers[config.ID]
	if ok {
		existing.Stop()

		collection.mtx.Lock()
		delete(collection.schedulers, config.ID)
		delete(collection.schedulerConfigs, config.ID)
		delete(collection.jobs, config.ID)
		collection.mtx.Unlock()
	}
}

// StopAll cron schedulers.
func (collection *CronCollection) StopAll() {
	collection.log(zerolog.DebugLevel).Msg("CronCollection.StopAll() is triggered. Stopping all cronjobs...")

	for _, existing := range collection.schedulers {
		existing.Stop()
	}
}

// StartAll cron schedulers.
func (collection *CronCollection) StartAll() {
	collection.log(zerolog.DebugLevel).Msg("CronCollection.StartAll() is triggered. Running all cronjobs...")

	for _, existing := range collection.schedulers {
		existing.StartAsync()
	}
	collection.log(zerolog.DebugLevel).Msg("CronCollection.StartAll() is triggered. Started all cronjobs asynchronously")
}

// AllStatuses shows all cron configurations.
// This is not correct. The information should somehow be in the database.
func (collection *CronCollection) AllStatuses() (map[string]CronStatus, error) {
	statuses := make(map[string]CronStatus)

	for id, _ := range collection.schedulerConfigs {
		status, err := collection.getStatus(id)
		if err != nil {
			return nil, err
		}

		statuses[id] = status
	}

	return statuses, nil
}
