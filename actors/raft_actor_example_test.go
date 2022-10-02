package actors_test

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ez-framework/ez-framework/actors"
	"github.com/ez-framework/ez-framework/configkv"
	"github.com/ez-framework/ez-framework/raft"
	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/nats-io/graft"
	"github.com/nats-io/nats.go"
	"golang.org/x/sync/errgroup"
)

func ExampleNewRaftActor() {
	httpAddr := ":3000"

	// Every Actor always subscribe to Nats JetStream
	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	jetstreamContext, _ := nc.JetStream()

	// Every Actor always store its config on JetStream's KV store
	confkv, _ := configkv.NewConfigKV(jetstreamContext)

	raftActorConfig := actors.ActorConfig{
		NatsAddr:         nats.DefaultURL,
		HTTPAddr:         httpAddr,
		NatsConn:         nc,
		JetStreamContext: jetstreamContext,
		ConfigKV:         confkv,
		StreamConfig: &nats.StreamConfig{
			MaxAge: 1 * time.Minute,
		},
	}

	// Always setup cancellation context so that Actor can shutdown properly
	ctx, done := context.WithCancel(context.Background())
	defer done()

	wg, ctx := errgroup.WithContext(ctx)

	// ConfigActor job is to receive config (from HTTP)
	raftActor, _ := actors.NewRaftActor(raftActorConfig)

	raftActor.OnBecomingLeader = func(state graft.State) {
		fmt.Printf("node is becoming a leader\n")
	}
	raftActor.OnBecomingFollower = func(state graft.State) {
		fmt.Printf("node is becoming a follower\n")
	}

	wg.Go(func() error {
		raftActor.RunConfigListener(ctx)
		return nil
	})

	raftActor.OnBootLoadConfig()

	// Running HTTP Server to modify or display config
	r := chi.NewRouter()
	r.Use(middleware.Logger)

	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"msg": "welcome"}`))
	})

	// POST /api/admin/raft
	// POST /api/admin/raft?command=UNSUB
	// sDELETE /api/admin/raft
	r.Method("POST", "/api/admin/raft", raftActor)
	r.Method("DELETE", "/api/admin/raft", raftActor)

	// GET method for raft metadata is handled by the underlying Raft struct
	r.Method("GET", "/api/admin/raft", raft.NewRaftHTTPGet(raftActor.Raft))

	r.Method("GET", "/api/admin/configkv", configkv.NewConfigKVHTTPGetAll(confkv))

	fmt.Printf("running an HTTP server...\n")
	httpServer := &http.Server{Addr: httpAddr, Handler: r}
	wg.Go(func() error {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	})

	// Listen to interrupts to cleanly kill Actors.
	wg.Go(func() error {
		signalChannel := make(chan os.Signal, 1)
		signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)

		select {
		case <-signalChannel:
			fmt.Printf("signal received\n")
			httpServer.Shutdown(ctx)
			done()

		case <-ctx.Done():
			fmt.Printf("closing signal goroutine\n")
			return ctx.Err()
		}

		return nil
	})

	// wait for all errgroup goroutines
	wg.Wait()
}
