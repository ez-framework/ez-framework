package main

import (
	"context"
	"errors"
	"flag"
	"net/http"
	"os"
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
	// How to connect to Jetstream

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
	// How to create KV store for configuration

	confkv, err := configkv.NewConfigKV(jetstreamContext)
	if err != nil {
		errLog.Fatal().Err(err).Msg("failed to setup KV store")
	}

	// ---------------------------------------------------------------------------
	// Proper signal handling with cancellation context and errgroup
	// This is needed to ensure that every actor finished cleanly before shutdown
	ctx, done := context.WithCancel(context.Background())
	defer done()

	wg, ctx := errgroup.WithContext(ctx)

	// ---------------------------------------------------------------------------
	// How to create a raft node as an actor

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
	// How to create cron scheduler as an actor

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
		cronActor.RunConfigListener(ctx)
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

	// ---------------------------------------------------------------------------
	// How to run cron scheduler only when this service is the leader

	raftActor.OnBecomingLeader = func(state graft.State) {
		cronActor.IsLeader <- true
	}
	raftActor.OnBecomingCandidate = func(state graft.State) {
	}
	raftActor.OnBecomingFollower = func(state graft.State) {
		cronActor.IsFollower <- true
	}

	wg.Go(func() error {
		raftActor.RunConfigListener(ctx)
		return nil
	})

	// ---------------------------------------------------------------------------
	// CronActor needs a target worker to execute the actual work
	// We will create a generic worker actor called hello
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
	workerActor.SetOnConfigUpdate(func(ctx context.Context, msg *nats.Msg) {
		// Pretend to do a big work
		outLog.Info().Str("subject", msg.Subject).Bytes("content", msg.Data).Msg("hello world!")
	})
	wg.Go(func() error {
		workerActor.RunConfigListener(ctx)
		return nil
	})

	// ---------------------------------------------------------------------------
	// Load all configurations on boot

	raftActor.OnBootLoadConfig()
	cronActor.OnBootLoadConfig()

	// ---------------------------------------------------------------------------
	// How to mount the HTTP handlers of each actor

	r := chi.NewRouter()
	r.Use(middleware.Logger)

	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("welcome"))
	})

	// Examples:
	//   POST /api/admin/raft   -> becomes command=UPDATE
	//   POST /api/admin/raft?command=UNSUB
	//   DELETE /api/admin/raft -> becomes command=DELETE
	r.Method("POST", "/api/admin/raft", raftActor)
	r.Method("DELETE", "/api/admin/raft", raftActor)

	// GET method for raft metadata is handled by the underlying Raft struct
	r.Method("GET", "/api/admin/raft", raft.NewRaftHTTPGet(raftActor.Raft))

	// Examples:
	//   POST /api/admin/cron
	//   DELETE /api/admin/cron
	r.Method("POST", "/api/admin/cron", cronActor)
	r.Method("DELETE", "/api/admin/cron", cronActor)

	// GET method for cron metadata is handled by the underlying CronCollection struct
	r.Method("GET", "/api/admin/cron", cron.NewCronCollectionHTTPGet(cronActor.CronCollection))

	// Shows all the config stored
	r.Method("GET", "/api/admin/configkv", configkv.NewConfigKVHTTPGetAll(confkv))

	httpServer := &http.Server{Addr: *httpAddr, Handler: r}

	wg.Go(func() error {
		outLog.Info().Str("http.addr", *httpAddr).Msg("running an HTTP server...")
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	})

	// -------------------------------------------------------
	// Handle shutdown signals

	// goroutine to check for signals to gracefully finish everything
	wg.Go(func() error {
		<-ctx.Done()
		outLog.Info().Msg("closing signal goroutine")
		httpServer.Shutdown(context.Background())
		return ctx.Err()
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
