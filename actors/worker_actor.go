package actors

import (
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
