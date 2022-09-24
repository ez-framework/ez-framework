package actors

import (
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
)

// NewWorkerActor is the constructor for WorkerActor
func NewWorkerActor(actorConfig ActorConfig, name string) (*WorkerActor, error) {
	name = "ez-worker-" + name

	actor := &WorkerActor{
		Actor: Actor{
			actorConfig: actorConfig,
			streamName:  name,
			infoLogger:  log.Info().Str("stream.name", name),
			errorLogger: log.Error().Str("stream.name", name),
			ConfigKV:    actorConfig.ConfigKV,
		},
	}

	// WorkerActor must use nats.WorkQueuePolicy.
	// We want the queueing behavior where a message is popped 1 by 1 by 1 random worker.
	if actor.actorConfig.StreamConfig.Retention != nats.WorkQueuePolicy {
		actor.actorConfig.StreamConfig.Retention = nats.WorkQueuePolicy
	}

	err := actor.setupStream()
	if err != nil {
		return nil, err
	}

	return actor, nil
}

// WorkerActor is a generic Actor.
type WorkerActor struct {
	Actor
}
