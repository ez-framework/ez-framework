package actors

import (
	"encoding/json"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"

	"github.com/ez-framework/ez-framework/config_internal"
	"github.com/ez-framework/ez-framework/configkv"
	"github.com/ez-framework/ez-framework/raft"
)

func NewRaftActor(jetstreamContext nats.JetStreamContext, confkv *configkv.ConfigKV) (*RaftActor, error) {
	name := "ez-raft"

	actor := &RaftActor{
		Actor: Actor{
			jc:            jetstreamContext,
			jetstreamName: name,
			infoLogger:    log.Info().Str("stream.name", name),
			errorLogger:   log.Error().Str("stream.name", name),
			ConfigKV:      confkv,
		},
	}

	err := actor.setupJetStreamStream()
	if err != nil {
		return nil, err
	}

	return actor, nil
}

type RaftActor struct {
	Actor
	raftNode *raft.Raft
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

			conf := config_internal.ConfigRaft{}

			err := json.Unmarshal(configBytes, &conf)
			if err != nil {
				actor.errorLogger.Err(err).Msg("failed to unmarshal config")
				return
			}

			// If there is an existing raftNode, close it.
			if actor.raftNode != nil {
				actor.raftNode.Close()
			}

			raftNode, err := raft.NewRaft(conf.ClusterName, conf.LogPath, conf.ClusterSize, conf.NatsAddr)
			if err != nil {
				actor.errorLogger.Err(err).Msg("failed to create a raft node")
			}
			actor.raftNode = raftNode

			actor.infoLogger.Msg("RaftActor is running")
			raftNode.Run()

		} else if actor.keyHasCommand(msg.Subject, "DELETE") {
			if actor.raftNode != nil {
				actor.raftNode.Close()
				actor.raftNode = nil
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

	err = actor.Publish("raft-node.command:POST", configBytes)
	if err != nil {
		actor.errorLogger.Err(err).Msg("failed to publish")
	}

	return err
}
