package cron

import (
	"time"

	"github.com/go-co-op/gocron"
)

type CronConfig struct {
	ID          string
	Schedule    string
	Timezone    string
	WorkerQueue string
}

type CronCollection struct {
	c              map[string]*gocron.Scheduler
	confCollection map[string]CronConfig
}

func (collection *CronCollection) Update(config CronConfig) {
	existing, ok := collection.c[config.ID]
	if ok {
		existing.Stop()
	}

	scheduler := gocron.NewScheduler(time.UTC) // TODO: elegantly assign timezone based on config
	scheduler.Cron(config.Schedule)
	collection.c[config.ID] = scheduler
	collection.confCollection[config.ID] = config

	scheduler.StartAsync()
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
