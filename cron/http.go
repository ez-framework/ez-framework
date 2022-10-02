package cron

import (
	"encoding/json"
	"net/http"

	"github.com/rs/zerolog/log"
)

func NewCronCollectionHTTPGet(collection *CronCollection) *CronCollectionHTTPGet {
	return &CronCollectionHTTPGet{collection: collection}
}

type CronCollectionHTTPGet struct {
	collection *CronCollection
}

func (handler *CronCollectionHTTPGet) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	if handler.collection == nil {
		w.Write([]byte(`{}`))
		return
	}

	allStatuses, err := handler.collection.AllStatuses()
	if err != nil {
		log.Error().Err(err).Msg("failed to render cronjob statuses")

		w.Write([]byte(`{}`))
		return
	}

	json.NewEncoder(w).Encode(allStatuses)
}
