package raft

import (
	"encoding/json"

	"github.com/rs/zerolog/log"

	"github.com/easy-framework/easy-framework/config_internal"
)

func NewRaftActor() *RaftActor {
	return &RaftActor{
		updateChannel: make(chan []byte),
	}
}

type RaftActor struct {
	updateChannel chan []byte
	raftNode      *Raft
}

func (ra *RaftActor) GetUpdateChannel() chan []byte {
	return ra.updateChannel
}

func (ra *RaftActor) Run() {
	infoLogger := log.Info()
	errLogger := log.Error()

	for {
		payload := <-ra.updateChannel
		infoLogger.Bytes("updateChannelPayload", payload).Msg("Received an update message")
		conf := config_internal.ConfigRaft{}

		err := json.Unmarshal(payload, &conf)
		if err != nil {
			errLogger.Err(err).Msg("Failed to unmarshal config")
		}

		if ra.raftNode != nil {
			ra.raftNode.Close()
		}

		raftNode, err := NewRaft(conf.ClusterName, conf.LogPath, conf.ClusterSize, conf.NatsAddr)
		if err != nil {
			errLogger.Err(err).Msg("failed to create a raft node")
		}
		ra.raftNode = raftNode

		raftNode.Run()
	}
}
