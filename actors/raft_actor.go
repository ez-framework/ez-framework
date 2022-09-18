package actors

import (
	"encoding/json"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"

	"github.com/ez-framework/ez-framework/raft"
)

func NewRaftActor(globalConfig GlobalConfig) (*RaftActor, error) {
	name := "ez-raft"

	actor := &RaftActor{
		Actor: Actor{
			globalConfig:  globalConfig,
			jc:            globalConfig.JetStreamContext,
			jetstreamName: name,
			infoLogger:    log.Info().Str("stream.name", name),
			errorLogger:   log.Error().Str("stream.name", name),
			ConfigKV:      globalConfig.ConfigKV,
		},
	}

	err := actor.setupJetStreamStream(&nats.StreamConfig{
		MaxAge: 1 * time.Minute,
	})
	if err != nil {
		return nil, err
	}

	return actor, nil
}

type RaftActor struct {
	Actor
	RaftNode *raft.Raft
}

func (actor *RaftActor) jetstreamSubscribeSubjects() string {
	return actor.jetstreamName + ".>"
}

func (actor *RaftActor) Run() {
	actor.infoLogger.
		Caller().
		Str("subjects.subscribe", actor.jetstreamSubscribeSubjects()).
		Msg("subscribing to nats subjects")

	actor.jc.Subscribe(actor.jetstreamSubscribeSubjects(), func(msg *nats.Msg) {
		if actor.keyHasCommand(msg.Subject, "POST") || actor.keyHasCommand(msg.Subject, "PUT") {
			configBytes := msg.Data

			conf := raft.ConfigRaft{}

			err := json.Unmarshal(configBytes, &conf)
			if err != nil {
				actor.errorLogger.Err(err).Msg("failed to unmarshal config")
				return
			}

			// Fill in config from actor's global config
			if conf.NatsAddr == "" {
				conf.NatsAddr = actor.globalConfig.NatsAddr
			}
			if conf.HTTPAddr == "" {
				conf.HTTPAddr = actor.globalConfig.HTTPAddr
			}

			// If there is an existing RaftNode, close it.
			if actor.RaftNode != nil {
				actor.RaftNode.Close()
			}

			raftNode, err := raft.NewRaft(conf)
			if err != nil {
				actor.errorLogger.Err(err).Msg("failed to create a raft node")
				return
			}
			actor.RaftNode = raftNode

			actor.infoLogger.Msg("RaftActor is running")
			actor.RaftNode.Run()

		} else if actor.keyHasCommand(msg.Subject, "DELETE") {
			if actor.RaftNode != nil {
				actor.RaftNode.Close()
			}
		}
	})
}

func (actor *RaftActor) OnBootLoad() error {
	configBytes, err := actor.ConfigKV.GetConfigBytes(actor.jetstreamName)
	if err != nil {
		actor.errorLogger.Err(err).Msg("failed to get config JSON bytes")
		return err
	}

	err = actor.Publish(actor.keyWithCommand(actor.jetstreamName, "POST"), configBytes)
	if err != nil {
		actor.errorLogger.Err(err).Msg("failed to publish")
	}

	return err
}
