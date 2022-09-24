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

	actor.SetPOSTSubscriber(actor.updateHandler)
	actor.SetPUTSubscriber(actor.updateHandler)
	actor.SetDELETESubscriber(actor.deleteHandler)

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

func (actor *RaftActor) updateHandler(msg *nats.Msg) {
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
	actor.Raft.RunSubscriberAsync()
}

// deleteHandler listens to DELETE command and do something
func (actor *RaftActor) deleteHandler(msg *nats.Msg) {
	err := actor.unsubscribeFromOnConfigUpdate()
	if err != nil {
		actor.errorLogger.Err(err).
			Err(err).
			Str("subjects", actor.subscribeSubjects()).
			Msg("failed to unsubscribe from subjects")
	}

	if actor.Raft != nil {
		actor.Raft.Close()
	}
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
