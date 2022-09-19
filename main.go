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
	"github.com/ez-framework/ez-framework/cron"
	"github.com/ez-framework/ez-framework/raft"
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

	// ---------------------------------------------------------------------------
	// Example on how to create KV store for configuration

	confkv, err := configkv.NewConfigKV(jetstreamContext)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to setup KV store")
	}

	// ---------------------------------------------------------------------------
	// common configuration for all actors
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
	// Example on how to create ConfigWSActor to push config to WS clients

	configWSActor, err := actors.NewConfigWSServerActor(globalActorConfig)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create ConfigWSActor")
	}
	go configWSActor.Run()

	// ---------------------------------------------------------------------------
	// Example on how to create a raft node as an actor

	raftActor, err := actors.NewRaftActor(globalActorConfig)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create raftActor")
	}
	go raftActor.Run()

	// ---------------------------------------------------------------------------
	// Example on how to create cron scheduler as an actor

	cronActor, err := actors.NewCronActor(globalActorConfig)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create cronActor")
	}
	go cronActor.Run()

	// ---------------------------------------------------------------------------
	// Example on how to load config on boot from KV store

	raftActor.OnBootLoad()
	cronActor.OnBootLoad()

	// ---------------------------------------------------------------------------
	// Example on how to mount the HTTP handlers of each actor

	r := chi.NewRouter()
	r.Use(middleware.Logger)

	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("welcome"))
	})
	r.Method("GET", "/api/admin/configkv", configkv.NewConfigKVHTTPGetAll(confkv))

	// As a common design, let the actor handles mutable HTTP methods
	// and leave the GET methods to the underlying struct.
	r.Method("POST", "/api/admin/configkv", configActor)
	r.Method("PUT", "/api/admin/configkv", configActor)
	r.Method("DELETE", "/api/admin/configkv", configActor)

	// Websocket handler to push config to clients.
	// Useful for IoT/edge services
	r.Handle("/api/admin/configkv/ws", configWSActor)

	// cronActor handles the creation and deletion of cron schedulers
	r.Method("POST", "/api/admin/cron", cronActor)
	r.Method("PUT", "/api/admin/cron", cronActor)
	r.Method("DELETE", "/api/admin/cron", cronActor)

	// GET method is handled by the underlying Raft struct
	r.Method("GET", "/api/admin/raft", raft.NewRaftHTTPGet(raftActor.RaftNode))

	// GET method is handled by the underlying CronCollection struct
	r.Method("GET", "/api/admin/cron", cron.NewCronCollectionHTTPGet(cronActor.CronCollection))

	log.Info().Str("http.addr", *httpAddr).Msg("running an HTTP server...")
	http.ListenAndServe(*httpAddr, r)
}
