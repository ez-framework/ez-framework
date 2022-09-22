package main

import (
	"encoding/json"
	"flag"
	"net/http"
	"os"

	"github.com/go-chi/chi/middleware"
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/ez-framework/ez-framework/actors"
)

func init() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
}

type memDB map[string][]byte

func (db memDB) Put(key string, value []byte) error {
	db[key] = value
	return nil
}

func (db memDB) Delete(key string) error {
	delete(db, key)
	return nil
}

type wsClientActorSettings struct {
	WSURL            string
	db               memDB
	OnPutChannels    [](chan []byte)
	OnDeleteChannels [](chan []byte)
}

func (settings wsClientActorSettings) GetWSURL() string {
	return settings.WSURL
}

func (settings wsClientActorSettings) GetKV() actors.IPutDelete {
	return settings.db
}

func (settings wsClientActorSettings) GetOnPutChannels() [](chan []byte) {
	return settings.OnPutChannels
}

func (settings wsClientActorSettings) GetOnDeleteChannels() [](chan []byte) {
	return settings.OnDeleteChannels
}

func main() {
	var (
		httpAddr = flag.String("http", ":4000", "HTTP address")
	)
	flag.Parse()

	// ---------------------------------------------------------------------------
	// Example on how to create ConfigWSClientActor to receive config from websocket server

	db := make(memDB)

	settings := wsClientActorSettings{
		WSURL: "ws://localhost:3000/api/admin/configkv/ws",
		db:    db,
	}
	configWSActor, err := actors.NewConfigWSClientActor(settings)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create ConfigWSActor")
	}
	go configWSActor.RunSubscriber()

	// ---------------------------------------------------------------------------
	// Example on how to mount the HTTP handlers of each actor

	r := chi.NewRouter()
	r.Use(middleware.Logger)

	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")

		payload := make(map[string]any)

		for key, jsonBytes := range db {
			var content any

			err := json.Unmarshal(jsonBytes, &content)
			if err == nil {
				payload[key] = content
			}
		}

		json.NewEncoder(w).Encode(payload)
	})

	log.Info().Str("http.addr", *httpAddr).Msg("running an HTTP server...")
	http.ListenAndServe(*httpAddr, r)
}
