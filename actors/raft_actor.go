package actors

import (
	"encoding/json"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"

	"github.com/ez-framework/ez-framework/config_internal"
	"github.com/ez-framework/ez-framework/raft"
)

func NewRaftActor(jetstreamContext nats.JetStreamContext) (*RaftActor, error) {
	ra := &RaftActor{
		Actor: Actor{
			jc:            jetstreamContext,
			jetstreamName: "ez-raft",
			commandChan:   make(chan *nats.Msg),
			infoLogger:    log.Info(),
			errorLogger:   log.Error(),
		},
		configKey: "ez-configlive.raft-node",
	}

	return ra, nil
}

type RaftActor struct {
	Actor
	configKey string
	raftNode  *raft.Raft
}

func (ra *RaftActor) Run() {
	subscription := ra.retrySubscribing(ra.jetstreamSubjects())
	defer subscription.Unsubscribe()

	ra.infoLoggerEvent().Msg("subscribing to nats subjects")

	// Wait until we get a new message
	for {
		msg := <-ra.commandChan

		ra.infoLoggerEvent().
			Str("msg.subject", msg.Subject).
			Bytes("msg.data", msg.Data).Msg("inspecting the content")

		if ra.keyHasCommand(msg.Subject, "POST") || ra.keyHasCommand(msg.Subject, "PUT") {
			configBytes := msg.Data

			conf := config_internal.ConfigRaft{}

			err := json.Unmarshal(configBytes, &conf)
			if err != nil {
				ra.errLoggerEvent(err).Msg("failed to unmarshal config")
				continue
			}

			// If there is an existing raftNode, close it.
			if ra.raftNode != nil {
				ra.raftNode.Close()
			}

			raftNode, err := raft.NewRaft(conf.ClusterName, conf.LogPath, conf.ClusterSize, conf.NatsAddr)
			if err != nil {
				ra.errLoggerEvent(err).Msg("failed to create a raft node")
			}
			ra.raftNode = raftNode

			ra.infoLoggerEvent().Msg("RaftActor is running")
			raftNode.Run()

		} else if ra.keyHasCommand(msg.Subject, "DELETE") {
			if ra.raftNode != nil {
				ra.raftNode.Close()
				ra.raftNode = nil
			}
		}
	}
}
