package actors

import (
	"encoding/json"

	"github.com/nats-io/graft"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"

	"github.com/ez-framework/ez-framework/raft"
)

// NewRaftActor is the constructor of RaftActor
func NewRaftActor(actorConfig ActorConfig) (*RaftActor, error) {
	name := "ez-raft"

	actor := &RaftActor{
		Actor: Actor{
			actorConfig: actorConfig,
			jc:          actorConfig.JetStreamContext,
			streamName:  name,
			infoLogger:  log.Info().Str("stream.name", name).Caller(),
			errorLogger: log.Error().Str("stream.name", name).Caller(),
			ConfigKV:    actorConfig.ConfigKV,
		},
	}

	err := actor.setupStream(actorConfig.StreamConfig)
	if err != nil {
		return nil, err
	}

	return actor, nil
}

type RaftActor struct {
	Actor
	Raft                *raft.Raft
	OnBecomingLeader    func(state graft.State)
	OnBecomingFollower  func(state graft.State)
	OnBecomingCandidate func(state graft.State)
	OnClosed            func(state graft.State)
}

// setRaft sets the raft object to the actor and also configure its hooks.
func (actor *RaftActor) setRaft(raftNode *raft.Raft) {
	actor.Raft = raftNode
	actor.Raft.OnBecomingLeader = actor.OnBecomingLeader
	actor.Raft.OnBecomingFollower = actor.OnBecomingFollower
	actor.Raft.OnBecomingCandidate = actor.OnBecomingCandidate
	actor.Raft.OnClosed = actor.OnClosed
}

// RunOnConfigUpdate listens to config changes and rebuild the raft consensus
func (actor *RaftActor) RunOnConfigUpdate() {
	actor.infoLogger.
		Caller().
		Str("subjects.subscribe", actor.subscribeSubjects()).
		Msg("subscribing to nats subjects")

	actor.jc.Subscribe(actor.subscribeSubjects(), func(msg *nats.Msg) {
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
				conf.NatsAddr = actor.actorConfig.NatsAddr
			}
			if conf.HTTPAddr == "" {
				conf.HTTPAddr = actor.actorConfig.HTTPAddr
			}

			// If there is an existing RaftNode, close it.
			if actor.Raft != nil {
				actor.Raft.Close()
			}

			raftNode, err := raft.NewRaft(conf)
			if err != nil {
				actor.errorLogger.Err(err).Msg("failed to create a raft node")
				return
			}
			actor.setRaft(raftNode)

			actor.infoLogger.Msg("RaftActor is running")
			actor.Raft.RunOnConfigUpdate()

		} else if actor.keyHasCommand(msg.Subject, "DELETE") {
			if actor.Raft != nil {
				actor.Raft.Close()
			}
		}
	})
}

// OnBootLoadConfig loads config from KV store and publish them so that we can build a consensus.
func (actor *RaftActor) OnBootLoadConfig() error {
	configBytes, err := actor.ConfigKV.GetConfigBytes(actor.streamName)
	if err != nil {
		actor.errorLogger.Err(err).Msg("failed to get config JSON bytes")
		return err
	}

	err = actor.Publish(actor.keyWithCommand(actor.streamName, "POST"), configBytes)
	if err != nil {
		actor.errorLogger.Err(err).Msg("failed to publish")
	}

	return err
}
