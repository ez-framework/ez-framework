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
			infoLogger:  log.Info().Str("stream.name", name).Caller(),
			errorLogger: log.Error().Str("stream.name", name).Caller(),
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
	POST   func(msg *nats.Msg)
	PUT    func(msg *nats.Msg)
	DELETE func(msg *nats.Msg)
}

// POSTSubscriber listens to POST command and do something
func (actor *WorkerActor) POSTSubscriber(msg *nats.Msg) {
	if actor.POST != nil {
		actor.POST(msg)
	}
}

// PUTSubscriber listens to PUT command and do something
func (actor *WorkerActor) PUTSubscriber(msg *nats.Msg) {
	if actor.PUT != nil {
		actor.PUT(msg)
	}
}

// DELETESubscriber listens to DELETE command and do something
func (actor *WorkerActor) DELETESubscriber(msg *nats.Msg) {
	if actor.DELETE != nil {
		actor.DELETE(msg)
	}
}
