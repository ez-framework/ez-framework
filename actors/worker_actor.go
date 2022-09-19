package actors

import (
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
)

func NewWorkerActor(globalConfig GlobalConfig, name string, instances int, runfunc func([]byte)) (*WorkerActor, error) {
	name = "ez-worker-" + name

	actor := &WorkerActor{
		Actor: Actor{
			globalConfig:  globalConfig,
			jc:            globalConfig.JetStreamContext,
			jetstreamName: name,
			infoLogger:    log.Info().Str("stream.name", name),
			errorLogger:   log.Error().Str("stream.name", name),
			ConfigKV:      globalConfig.ConfigKV,
		},
		Instances:   instances,
		RunFunction: runfunc,
	}

	err := actor.setupJetStreamStream(&nats.StreamConfig{
		MaxAge: 1 * time.Minute,
	})
	if err != nil {
		return nil, err
	}

	return actor, nil
}

type WorkerActor struct {
	Actor
	Instances   int
	RunFunction func([]byte)
}

func (actor *WorkerActor) jetstreamSubscribeSubjects() string {
	return actor.jetstreamName + ".>"
}

func (actor *WorkerActor) Run() {
	for i := 0; i < actor.Instances; i++ {
		actor.infoLogger.
			Caller().
			Int("index", i).
			Str("subjects.subscribe", actor.jetstreamSubscribeSubjects()).
			Msg("subscribing to nats subjects")

		actor.jc.QueueSubscribe(actor.jetstreamSubscribeSubjects(), "workers", func(msg *nats.Msg) {
			actor.RunFunction(msg.Data)
		})
	}

	// TODO: book keeping the subscription to unsubscribe later
}

func (actor *WorkerActor) OnBootLoad() error {
	return nil
}
