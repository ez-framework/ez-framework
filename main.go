package main

import (
	"context"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/go-chi/chi/middleware"
	"github.com/go-chi/chi/v5"
	"github.com/nats-io/graft"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"

	"github.com/ez-framework/ez-framework/actors"
	"github.com/ez-framework/ez-framework/configkv"
	"github.com/ez-framework/ez-framework/cron"
	"github.com/ez-framework/ez-framework/http_logs"
	"github.com/ez-framework/ez-framework/raft"
)

var outLog zerolog.Logger
var errLog zerolog.Logger

func init() {
	outLog = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).With().Timestamp().Logger()
	errLog = zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}).With().Timestamp().Logger()
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
		errLog.Fatal().Err(err).Msg("failed to connect to nats")
	}
	defer nc.Close()

	jetstreamContext, err := nc.JetStream()
	if err != nil {
		errLog.Fatal().Err(err).Msg("failed to get jetstream context")
	}

	// ---------------------------------------------------------------------------
	// Example on how to create KV store for configuration

	confkv, err := configkv.NewConfigKV(jetstreamContext)
	if err != nil {
		errLog.Fatal().Err(err).Msg("failed to setup KV store")
	}

	// ---------------------------------------------------------------------------
	// Misc setup

	// This is needed to ensure that every actor finished cleanly before shutdown
	ctx, cancelFunc := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	logStreamMaxAge := 10 * time.Minute

	// ---------------------------------------------------------------------------
	// Example on how to create ConfigActor
	configActorConfig := actors.ActorConfig{
		WaitGroup:        wg,
		Context:          ctx,
		NatsAddr:         *natsAddr,
		HTTPAddr:         *httpAddr,
		NatsConn:         nc,
		JetStreamContext: jetstreamContext,
		ConfigKV:         confkv,
		StreamConfig: &nats.StreamConfig{
			MaxAge:    1 * time.Minute,
			Retention: nats.WorkQueuePolicy,
		},
	}
	configActor, err := actors.NewConfigActor(configActorConfig)
	if err != nil {
		errLog.Fatal().Err(err).Msg("failed to create ConfigActor")
	}
	configActor.RunSubscriberAsync()

	// ---------------------------------------------------------------------------
	// Example on how to create ConfigWSActor to push config to WS clients
	configWSActorConfig := actors.ActorConfig{
		WaitGroup:        wg,
		Context:          ctx,
		NatsAddr:         *natsAddr,
		HTTPAddr:         *httpAddr,
		NatsConn:         nc,
		JetStreamContext: jetstreamContext,
		ConfigKV:         confkv,
		StreamConfig: &nats.StreamConfig{
			MaxAge:    1 * time.Minute,
			Retention: nats.WorkQueuePolicy,
		},
	}
	configWSActor, err := actors.NewConfigWSServerActor(configWSActorConfig)

	if err != nil {
		errLog.Fatal().Err(err).Msg("failed to create ConfigWSActor")
	}
	configWSActor.RunSubscriberAsync()

	// ---------------------------------------------------------------------------
	// Example on how to create a raft node as an actor
	raftActorConfig := actors.ActorConfig{
		WaitGroup:        wg,
		Context:          ctx,
		NatsAddr:         *natsAddr,
		HTTPAddr:         *httpAddr,
		NatsConn:         nc,
		JetStreamContext: jetstreamContext,
		ConfigKV:         confkv,
		StreamConfig: &nats.StreamConfig{
			MaxAge: 1 * time.Minute,
		},
		LogsStreamConfig: &nats.StreamConfig{
			MaxAge: logStreamMaxAge,
		},
	}
	raftActor, err := actors.NewRaftActor(raftActorConfig)
	if err != nil {
		errLog.Fatal().Err(err).Msg("failed to create raftActor")
	}
	raftActor.RunSubscriberAsync()

	// ---------------------------------------------------------------------------
	// Example on how to create cron scheduler as an actor
	cronActorConfig := actors.ActorConfig{
		WaitGroup:        wg,
		Context:          ctx,
		NatsAddr:         *natsAddr,
		HTTPAddr:         *httpAddr,
		NatsConn:         nc,
		JetStreamContext: jetstreamContext,
		ConfigKV:         confkv,
		StreamConfig: &nats.StreamConfig{
			MaxAge: 1 * time.Minute,
		},
	}
	cronActor, err := actors.NewCronActor(cronActorConfig)
	if err != nil {
		errLog.Fatal().Err(err).Msg("failed to create cronActor")
	}
	cronActor.RunSubscriberAsync()
	go cronActor.OnBecomingLeaderSync()
	go cronActor.OnBecomingFollowerSync()
	cronActor.OnBootLoadConfig()

	// ---------------------------------------------------------------------------
	// Example on how to run cron scheduler only when this service is the leader
	raftActor.OnBecomingLeader = func(state graft.State) {
		errLog.Debug().Msg("node is becoming a leader")
		cronActor.IsLeader <- true
	}
	raftActor.OnBecomingFollower = func(state graft.State) {
		errLog.Debug().Msg("node is becoming a follower")
		cronActor.IsFollower <- true
	}

	raftActor.OnBootLoadConfig()

	// ---------------------------------------------------------------------------
	// Example on how to create a generic worker as an actor
	workerActorConfig := actors.ActorConfig{
		WaitGroup:        wg,
		Context:          ctx,
		NatsAddr:         *natsAddr,
		HTTPAddr:         *httpAddr,
		NatsConn:         nc,
		JetStreamContext: jetstreamContext,
		ConfigKV:         confkv,
		StreamConfig: &nats.StreamConfig{
			MaxAge:    1 * time.Minute,
			Retention: nats.WorkQueuePolicy,
		},
	}
	workerActor, err := actors.NewWorkerActor(workerActorConfig, "hello")
	if err != nil {
		errLog.Fatal().Err(err).Msg("failed to create workerActor")
	}
	workerActor.SetPOSTSubscriber(func(msg *nats.Msg) {
		outLog.Info().Str("subject", msg.Subject).Bytes("content", msg.Data).Msg("hello world!")
	})

	workerActor.RunSubscriberAsync()

	// ---------------------------------------------------------------------------
	// Example on how to mount the HTTP handlers of each actor

	r := chi.NewRouter()
	r.Use(middleware.Logger)

	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("welcome"))
	})

	// As a common design, let the actor handles mutable HTTP methods
	// and leave the GET methods to the underlying struct.
	r.Method("POST", "/api/admin/configkv", configActor)
	r.Method("PUT", "/api/admin/configkv", configActor)
	r.Method("DELETE", "/api/admin/configkv", configActor)
	r.Method("UNSUB", "/api/admin/configkv", configActor)

	r.Method("GET", "/api/admin/configkv", configkv.NewConfigKVHTTPGetAll(confkv))

	// Websocket handler to push config to clients.
	// Useful for IoT/edge services
	r.Handle("/api/admin/configkv/ws", configWSActor)

	// cronActor handles the creation and deletion of cron schedulers
	r.Method("POST", "/api/admin/cron", cronActor)
	r.Method("PUT", "/api/admin/cron", cronActor)
	r.Method("DELETE", "/api/admin/cron", cronActor)
	r.Method("UNSUB", "/api/admin/cron", cronActor)

	// GET method for raft metadata is handled by the underlying Raft struct
	r.Method("GET", "/api/admin/raft", raft.NewRaftHTTPGet(raftActor.Raft))

	// GET method for displaying cron metadata is handled by the underlying CronCollection struct
	r.Method("GET", "/api/admin/cron", cron.NewCronCollectionHTTPGet(cronActor.CronCollection))

	// GET method for displaying logs
	natsLogger, err := http_logs.NewHTTPLogs(http_logs.HTTPLogsConfig{
		JetStreamContext: jetstreamContext,
		StreamConfig: &nats.StreamConfig{
			MaxAge: logStreamMaxAge,
		},
	})
	if err == nil {
		r.Method("GET", "/api/admin/logs", natsLogger)
	}

	outLog.Info().Str("http.addr", *httpAddr).Msg("running an HTTP server...")
	go http.ListenAndServe(*httpAddr, r)

	// -------------------------------------------------------
	// Handle shutdown signals

	terminateChannel := make(chan os.Signal, 1)
	signal.Notify(terminateChannel, syscall.SIGINT, syscall.SIGTERM)

	<-terminateChannel
	outLog.Info().Msg("shutdown signal received")

	cancelFunc() // Signal cancellation to context.Context
	wg.Wait()    // Block here until are workers are done
	outLog.Info().Msg("all workers are done, shutting down...")
}
