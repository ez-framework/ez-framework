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
	ra := &RaftActor{
		Actor: Actor{
			jc:            jetstreamContext,
			jetstreamName: "ez-configlive",
			infoLogger:    log.Info(),
			errorLogger:   log.Error(),
			ConfigKV:      confkv,
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

func (ra *RaftActor) jetstreamSubjects() string {
	return ra.jetstreamName + ".raft-node.>"
}

func (ra *RaftActor) runNewRaftNode(configBytes []byte) {
	conf := config_internal.ConfigRaft{}

	err := json.Unmarshal(configBytes, &conf)
	if err != nil {
		ra.errLoggerEvent(err).Msg("failed to unmarshal config")
		return
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
}

func (ra *RaftActor) Run() {
	ra.infoLoggerEvent().Msg("Subscribing to nats subjects")

	ra.jc.Subscribe(ra.jetstreamSubjects(), func(msg *nats.Msg) {
		ra.infoLoggerEvent().
			Str("msg.subject", msg.Subject).
			Bytes("msg.data", msg.Data).Msg("inspecting the content")

		if ra.keyHasCommand(msg.Subject, "POST") || ra.keyHasCommand(msg.Subject, "PUT") {
			configBytes := msg.Data

			conf := config_internal.ConfigRaft{}

			err := json.Unmarshal(configBytes, &conf)
			if err != nil {
				ra.errLoggerEvent(err).Msg("failed to unmarshal config")
				return
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
	})
}

func (ra *RaftActor) OnBootLoad() error {
	configBytes, err := ra.ConfigKV.GetConfigBytes(ra.configKey)
	if err != nil {
		ra.errLoggerEvent(err).Msg("failed to get config JSON bytes")
		return err
	}

	println(string(configBytes))

	err = ra.Publish("ez-configlive.raft-node.command:POST", configBytes)
	if err != nil {
		ra.errLoggerEvent(err).Msg("failed to publish")
	}

	return err
}
