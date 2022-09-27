package main

import (
	"context"
	"errors"
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
)

var outLog zerolog.Logger
var errLog zerolog.Logger
var dbgLog zerolog.Logger

func init() {
	outLog = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).With().Timestamp().Logger()
	errLog = zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}).With().Timestamp().Logger()
	dbgLog = zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}).With().Timestamp().Logger()
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
	ctx, done := context.WithCancel(context.Background())
	wg, ctx := errgroup.WithContext(ctx)

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
	workerActor.SetSubscribers("POST", func(ctx context.Context, msg *nats.Msg) {
		outLog.Info().Str("subject", msg.Subject).Bytes("content", msg.Data).Msg("hello world!")
	})
	wg.Go(func() error {
		workerActor.RunSubscribersBlocking(ctx)
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
	}
	raftActor, err := actors.NewRaftActor(raftActorConfig)
	if err != nil {
		errLog.Fatal().Err(err).Msg("failed to create raftActor")
	}

	// ---------------------------------------------------------------------------
	// Example on how to run cron scheduler only when this service is the leader
	raftActor.OnBecomingLeader = func(state graft.State) {
		dbgLog.Debug().Msg("node is becoming a leader")
	}
	raftActor.OnBecomingFollower = func(state graft.State) {
		dbgLog.Debug().Msg("node is becoming a follower")
	}

	wg.Go(func() error {
		raftActor.RunSubscribersBlocking(ctx)
		return nil
	})

	raftActor.OnBootLoadConfig()

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

	// GET method for raft metadata is handled by the underlying Raft struct
	// r.Method("GET", "/api/admin/raft", raft.NewRaftHTTPGet(raftActor.Raft))

	outLog.Info().Str("http.addr", *httpAddr).Msg("running an HTTP server...")
	httpServer := &http.Server{Addr: *httpAddr, Handler: r}
	wg.Go(func() error {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	})

	// -------------------------------------------------------
	// Handle shutdown signals

	// goroutine to check for signals to gracefully finish everything
	wg.Go(func() error {
		signalChannel := make(chan os.Signal, 1)
		signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)

		select {
		case sig := <-signalChannel:
			outLog.Info().Str("signal", sig.String()).Msg("signal received")
			httpServer.Shutdown(ctx)
			done()

		case <-ctx.Done():
			outLog.Info().Msg("closing signal goroutine")
			return ctx.Err()
		}

		return nil
	})

	// wait for all errgroup goroutines
	err = wg.Wait()
	if err != nil {
		switch {
		case errors.Is(err, context.Canceled):
			errLog.Error().Err(err).Msg("context was canceled")
		default:
			errLog.Error().Err(err).Msg("received error")
		}

	} else {
		outLog.Info().Msg("all workers are done, shutting down..")
	}
}
