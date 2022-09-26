package actors

import (
	"github.com/nats-io/nats.go"
)

// NewWorkerActor is the constructor for WorkerActor
func NewWorkerActor(actorConfig ActorConfig, name string) (*WorkerActor, error) {
	name = "ez-worker-" + name

	actor := &WorkerActor{
		Actor: Actor{
			actorConfig: actorConfig,
			streamName:  name,
			ConfigKV:    actorConfig.ConfigKV,
		},
	}

	// WorkerActor must use nats.WorkQueuePolicy.
	// We want the queueing behavior where a message is popped 1 by 1 by 1 random worker.
	if actor.actorConfig.StreamConfig.Retention != nats.WorkQueuePolicy {
		actor.actorConfig.StreamConfig.Retention = nats.WorkQueuePolicy
	}

	err := actor.setupConstructor()
	if err != nil {
		return nil, err
	}

	return actor, nil
}

// WorkerActor is a generic Actor.
type WorkerActor struct {
	Actor
}
