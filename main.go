package main

import (
	"context"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/middleware"
	"github.com/go-chi/chi/v5"
	"github.com/nats-io/graft"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"

	"github.com/ez-framework/ez-framework/actors"
	"github.com/ez-framework/ez-framework/configkv"
	"github.com/ez-framework/ez-framework/cron"
	"github.com/ez-framework/ez-framework/http_logs"
	"github.com/ez-framework/ez-framework/raft"
)

var outLog zerolog.Logger
var errLog zerolog.Logger
var dbgLog zerolog.Logger
var logStreamMaxAge time.Duration

func init() {
	outLog = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).With().Timestamp().Logger()
	errLog = zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}).With().Timestamp().Logger()
	dbgLog = zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}).With().Timestamp().Logger()

	logStreamMaxAge = 10 * time.Minute
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
	ctx, cancel := context.WithCancel(context.Background())
	wg, ctx := errgroup.WithContext(ctx)

	terminateChannel := make(chan os.Signal, 1)
	signal.Notify(terminateChannel, syscall.SIGINT, syscall.SIGTERM)

	// ---------------------------------------------------------------------------
	// Example on how to create ConfigActor
	configActorConfig := actors.ActorConfig{
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
	wg.Go(func() error {
		configActor.RunSubscribersBlocking(ctx)
		return nil
	})

	// ---------------------------------------------------------------------------
	// Example on how to create ConfigWSActor to push config to WS clients
	configWSActorConfig := actors.ActorConfig{
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
	wg.Go(func() error {
		configWSActor.RunSubscribersBlocking(ctx)
		return nil
	})

	// ---------------------------------------------------------------------------
	// Example on how to create a raft node as an actor
	raftActorConfig := actors.ActorConfig{
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
	wg.Go(func() error {
		raftActor.RunSubscribersBlocking(ctx)
		return nil
	})

	// ---------------------------------------------------------------------------
	// Example on how to create cron scheduler as an actor
	cronActorConfig := actors.ActorConfig{
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
	wg.Go(func() error {
		cronActor.RunSubscribersBlocking(ctx)
		return nil
	})
	wg.Go(func() error {
		cronActor.OnBecomingLeaderBlocking(ctx)
		return nil
	})
	wg.Go(func() error {
		cronActor.OnBecomingFollowerBlocking(ctx)
		return nil
	})

	cronActor.OnBootLoadConfig()

	// ---------------------------------------------------------------------------
	// Example on how to run cron scheduler only when this service is the leader
	raftActor.OnBecomingLeader = func(state graft.State) {
		dbgLog.Debug().Msg("node is becoming a leader")
		cronActor.IsLeader <- true
	}
	raftActor.OnBecomingFollower = func(state graft.State) {
		dbgLog.Debug().Msg("node is becoming a follower")
		cronActor.IsFollower <- true
	}

	raftActor.OnBootLoadConfig()

	// ---------------------------------------------------------------------------
	// Example on how to create a generic worker as an actor
	workerActorConfig := actors.ActorConfig{
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
	wg.Go(func() error {
		workerActor.RunSubscribersBlocking(ctx)
		return nil
	})

	// ---------------------------------------------------------------------------
	// Example on how to mount the HTTP handlers of each actor

	r := chi.NewRouter()
	r.Use(middleware.Logger)

	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("welcome"))
	})

	// As a common design, let the actor handles mutable HTTP methods
	// and leave the GET methods to the underlying struct.
	// Because we are limited in terms of HTTP verbs, we'll use command parameter.
	// Examples:
	//   POST /api/admin/configkv?command=POST
	//   POST /api/admin/configkv?command=UNSUB
	r.Method("POST", "/api/admin/configkv", configActor)
	r.Method("DELETE", "/api/admin/configkv", configActor)

	r.Method("GET", "/api/admin/configkv", configkv.NewConfigKVHTTPGetAll(confkv))

	// Websocket handler to push config to clients.
	// Useful for IoT/edge services
	r.Handle("/api/admin/configkv/ws", configWSActor)

	// cronActor handles the creation and deletion of cron schedulers
	r.Method("POST", "/api/admin/cron", cronActor)
	r.Method("DELETE", "/api/admin/cron", cronActor)

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
	httpServer := &http.Server{Addr: *httpAddr, Handler: r}
	wg.Go(func() error {
		return httpServer.ListenAndServe()
	})

	// -------------------------------------------------------
	// Handle shutdown signals

	<-terminateChannel
	outLog.Info().Msg("shutdown signal received")

	go httpServer.Shutdown(ctx) // Shutdown HTTP server

	cancel() // Signal cancellation to context.Context

	wg.Wait() // Block here until are workers are done
	outLog.Info().Msg("all workers are done, shutting down...")
}
