package actors

import (
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
)

// NewWorkerActor is the constructor for WorkerActor
func NewWorkerActor(actorConfig ActorConfig, name string, instances int, runfunc func([]byte)) (*WorkerActor, error) {
	name = "ez-worker-" + name

	actor := &WorkerActor{
		Actor: Actor{
			actorConfig: actorConfig,
			jc:          actorConfig.JetStreamContext,
			streamName:  name,
			infoLogger:  log.Info().Str("stream.name", name),
			errorLogger: log.Error().Str("stream.name", name),
			ConfigKV:    actorConfig.ConfigKV,
		},
		Instances:   instances,
		RunFunction: runfunc,
	}

	err := actor.setupStream(&nats.StreamConfig{
		MaxAge: 1 * time.Minute,
	})
	if err != nil {
		return nil, err
	}

	return actor, nil
}

// WorkerActor
type WorkerActor struct {
	Actor
	Instances   int
	RunFunction func([]byte)
}

// RunOnConfigUpdate
func (actor *WorkerActor) RunOnConfigUpdate() {
	for i := 0; i < actor.Instances; i++ {
		actor.infoLogger.
			Caller().
			Int("index", i).
			Str("subjects.subscribe", actor.subscribeSubjects()).
			Msg("subscribing to nats subjects")

		actor.jc.QueueSubscribe(actor.subscribeSubjects(), "workers", func(msg *nats.Msg) {
			actor.RunFunction(msg.Data)
		})
	}

	// TODO: book keeping the subscription to unsubscribe later
}

// OnBootLoadConfig
func (actor *WorkerActor) OnBootLoadConfig() error {
	return nil
}
