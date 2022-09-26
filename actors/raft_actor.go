package actors

import (
	"encoding/json"

	"github.com/nats-io/graft"
	"github.com/nats-io/nats.go"

	"github.com/ez-framework/ez-framework/raft"
)

// NewRaftActor is the constructor of RaftActor
func NewRaftActor(actorConfig ActorConfig) (*RaftActor, error) {
	name := "ez-raft"

	actor := &RaftActor{
		Actor: Actor{
			actorConfig: actorConfig,
			streamName:  name,
			ConfigKV:    actorConfig.ConfigKV,
		},
	}

	// RaftActor cannot use nats.WorkQueuePolicy.
	// Config must be published to all subscribers because we don't know which instance is the leader.
	if actor.actorConfig.StreamConfig.Retention == nats.WorkQueuePolicy {
		actor.actorConfig.StreamConfig.Retention = nats.LimitsPolicy
	}

	err := actor.setupConstructor()
	if err != nil {
		return nil, err
	}

	actor.SetPOSTSubscriber(actor.updateHandler)
	actor.SetPUTSubscriber(actor.updateHandler)
	actor.SetDELETESubscriber(actor.deleteHandler)
	actor.SetOnDoneSubscribing(actor.doneSubscribingHandler)

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

// updateHandler receives a new raft config, and starts participating in Raft quorum
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

	actor.infoLogger.Msg("RaftActor is running")
	go actor.Raft.RunSubscribersBlocking()
}

// deleteHandler listens to DELETE command and stop participating in Raft quorum
func (actor *RaftActor) deleteHandler(msg *nats.Msg) {
	actor.doneSubscribingHandler()
}

// doneSubscribingHandler
func (actor *RaftActor) doneSubscribingHandler() {
	println("am i called? doneSubscribingHandler()")
	// Stop listening to JetStream config changes.
	err := actor.Unsubscribe()
	if err != nil {
		actor.errorLogger.Err(err).
			Err(err).
			Str("subjects", actor.subscribeSubjects()).
			Msg("failed to unsubscribe from subjects")
	}

	// Close the raft
	if actor.Raft != nil {
		println("am i called? doneSubscribingHandler() Raft.Close()")
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
