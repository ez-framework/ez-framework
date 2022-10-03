package main

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

	"github.com/ez-framework/ez-framework/actors"
)

var outLog zerolog.Logger
var errLog zerolog.Logger

func init() {
	outLog = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).With().Timestamp().Logger()
	errLog = zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}).With().Timestamp().Logger()
}

func main() {
	wsURL := "ws://localhost:3000/api/admin/ws/worker/hello"

	// ---------------------------------------------------------------------------
	// Proper signal handling with cancellation context and errgroup
	// This is needed to ensure that every actor finished cleanly before shutdown
	ctx, done := context.WithCancel(context.Background())
	defer done()

	wg, ctx := errgroup.WithContext(ctx)

	// -----------------------------------------------------------------------------------
	// Create a WorkerWSActor to receive parameters from websocket server and perform work

	workerWSActor, err := actors.NewWorkerWSActor(wsURL)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create workerWSActor")
	}
	workerWSActor.SetOnConfigUpdate(func(ctx context.Context, msg *nats.Msg) {
		// Pretend to do a big work
		outLog.Info().
			Str("subject", msg.Subject).
			Bytes("parameters", msg.Data).
			Msg("hello world from websocket worker!")
	})
	wg.Go(func() error {
		workerWSActor.RunConfigListener(ctx)
		return nil
	})

	// -------------------------------------------------------
	// Handle shutdown signals

	// goroutine to check for signals to gracefully finish everything
	wg.Go(func() error {
		<-ctx.Done()
		outLog.Info().Msg("closing signal goroutine")
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
