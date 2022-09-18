package main

import (
	"flag"
	"net/http"
	"os"

	"github.com/go-chi/chi/middleware"
	"github.com/go-chi/chi/v5"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/ez-framework/ez-framework/actors"
	"github.com/ez-framework/ez-framework/configkv"
)

func init() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
}

func main() {
	var (
		natsAddr = flag.String("nats", nats.DefaultURL, "NATS address")
		httpAddr = flag.String("http", ":3000", "HTTP address")
	)
	flag.Parse()

	// ---------------------------------------------------------------------------
	// Example on how to connect to Jetstream

	nc, err := nats.Connect(*natsAddr)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to connect to nats")
	}
	defer nc.Close()

	jetstreamContext, err := nc.JetStream()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to get jetstream context")
	}

	confkv, err := configkv.NewConfigKV(jetstreamContext)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to setup KV store")
	}

	globalActorConfig := actors.GlobalConfig{
		NatsAddr:         *natsAddr,
		HTTPAddr:         *httpAddr,
		NatsConn:         nc,
		JetStreamContext: jetstreamContext,
		ConfigKV:         confkv,
	}

	// ---------------------------------------------------------------------------
	// Example on how to create ConfigActor

	configActor, err := actors.NewConfigActor(globalActorConfig)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create ConfigActor")
	}
	go configActor.Run()

	// ---------------------------------------------------------------------------
	// Example on how to create a raft node as an actor

	raftActor, err := actors.NewRaftActor(globalActorConfig)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create raftActor")
	}
	go raftActor.Run()

	// ---------------------------------------------------------------------------
	// Example on how to load config on boot from KV store

	raftActor.OnBootLoad()

	// ---------------------------------------------------------------------------
	// Example on how to mount the HTTP handlers of each actor

	r := chi.NewRouter()
	r.Use(middleware.Logger)

	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("welcome"))
	})
	r.Method("GET", "/api/admin/configkv", configkv.NewConfigKVHTTPGetAll(confkv))
	r.Method("POST", "/api/admin/configkv", configActor)
	r.Method("PUT", "/api/admin/configkv", configActor)
	r.Method("DELETE", "/api/admin/configkv", configActor)

	log.Info().Str("http.addr", *httpAddr).Msg("Running an HTTP server...")
	http.ListenAndServe(*httpAddr, r)
}
