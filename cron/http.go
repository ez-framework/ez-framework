package cron

import (
	"encoding/json"
	"net/http"
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

	allConfigs := handler.collection.AllStatuses()

	json.NewEncoder(w).Encode(allConfigs)
}
