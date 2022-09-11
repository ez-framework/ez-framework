package raft

import (
	"encoding/json"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"

	"github.com/ez-framework/ez-framework/config_internal"
)

func NewRaftActor(jetstreamContext nats.JetStreamContext) *RaftActor {
	return &RaftActor{
		jc: jetstreamContext,
	}
}

type RaftActor struct {
	jc       nats.JetStreamContext
	raftNode *Raft
}

func (ra *RaftActor) Run() {
	infoLogger := log.Info()
	errLogger := log.Error()

	configKey := config_internal.ConfigRaftKey

	ra.jc.Subscribe(configKey, func(msg *nats.Msg) {
		configBytes := msg.Data

		infoLogger.Str("configKey", configKey).Bytes("configBytes", configBytes).Msg("Received an update message")
		conf := config_internal.ConfigRaft{}

		err := json.Unmarshal(configBytes, &conf)
		if err != nil {
			errLogger.Err(err).Msg("Failed to unmarshal config")
			return
		}

		if ra.raftNode != nil {
			ra.raftNode.Close()
		}

		raftNode, err := NewRaft(conf.ClusterName, conf.LogPath, conf.ClusterSize, conf.NatsAddr)
		if err != nil {
			errLogger.Err(err).Msg("failed to create a raft node")
		}
		ra.raftNode = raftNode

		infoLogger.Msg("RaftActor is running")
		raftNode.Run()
	})
}
