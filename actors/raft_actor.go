package actors

import (
	"context"
	"encoding/json"

	"github.com/nats-io/graft"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"

	"github.com/ez-framework/ez-framework/raft"
)

// NewRaftActor is the constructor of RaftActor
func NewRaftActor(actorConfig ActorConfig) (*RaftActor, error) {
	name := "ez-raft"

	actor := &RaftActor{
		Actor: Actor{
			config:     actorConfig,
			streamName: name,
			ConfigKV:   actorConfig.ConfigKV,
		},
	}

	// RaftActor cannot use nats.WorkQueuePolicy.
	// Config must be published to all subscribers because we don't know which instance is the leader.
	if actor.config.Nats.StreamConfig.Retention == nats.WorkQueuePolicy {
		actor.config.Nats.StreamConfig.Retention = nats.LimitsPolicy
	}

	err := actor.setupConstructor()
	if err != nil {
		return nil, err
	}

	actor.SetOnConfigUpdate(actor.configUpdateHandler)
	actor.SetOnConfigDelete(actor.configDeleteHandler)

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

// configUpdateHandler receives a new raft config, and starts participating in Raft quorum
func (actor *RaftActor) configUpdateHandler(ctx context.Context, msg *nats.Msg) {
	configBytes := msg.Data

	conf := raft.ConfigRaft{}

	err := json.Unmarshal(configBytes, &conf)
	if err != nil {
		actor.errorLogger.Err(err).Msg("failed to unmarshal config")
		return
	}

	// Fill in config from actor's global config
	if conf.NatsAddr == "" {
		conf.NatsAddr = actor.config.Nats.Addr
	}
	if conf.HTTPAddr == "" {
		conf.HTTPAddr = actor.config.HTTPAddr
	}

	// If there is an existing Raft node, close it.
	if actor.Raft != nil {
		actor.Raft.Close()
	}

	raftNode, err := raft.NewRaft(conf)
	if err != nil {
		actor.errorLogger.Err(err).Msg("failed to create a raft node")
		return
	}
	actor.setRaft(raftNode)

	actor.log(zerolog.InfoLevel).Msg("RaftActor is running")
	go actor.Raft.RunBlocking(ctx)
}

// configDeleteHandler listens to DELETE command and stop participating in Raft quorum
func (actor *RaftActor) configDeleteHandler(ctx context.Context, msg *nats.Msg) {
	// Close the raft
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

	err = actor.PublishConfig(actor.keyWithCommand(actor.streamName, "UPDATE"), configBytes)
	if err != nil {
		actor.errorLogger.Err(err).Msg("failed to publish")
	}

	return err
}
