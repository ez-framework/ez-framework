package main

import (
	"flag"
	"os"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/easy-framework/easy-framework/config_internal"
	"github.com/easy-framework/easy-framework/config_live"
	"github.com/easy-framework/easy-framework/raft"
)

func init() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
}

func main() {
	var (
		logPath     = flag.String("path", "./.data/graft.log", "Raft log path")
		clusterName = flag.String("cluster", "cluster", "Cluster name")
		clusterSize = flag.Int("size", 3, "Cluster size")
		natsAddr    = flag.String("nats", nats.DefaultURL, "NATS address")
	)
	flag.Parse()

	// ---------------------------------------------------------------------------
	// Example on how to connect to Jetstream

	nc, err := nats.Connect(*natsAddr)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to connect to nats")
	}
	defer nc.Close()

	jetstreamContext, err := nc.JetStream()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to get jetstream context")
	}

	liveconf, err := config_live.NewConfigLive(jetstreamContext)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create liveconf")
	}
	go liveconf.SubscribeConfigUpdate()

	// ---------------------------------------------------------------------------
	// Example on how to create a raft node and participate in membership

	raftActor := raft.NewRaftActor()
	go raftActor.Run()

	raftInternalConfig := config_internal.ConfigRaft{
		LogPath:     *logPath,
		ClusterName: *clusterName,
		ClusterSize: *clusterSize,
		NatsAddr:    *natsAddr,
	}
	raftInternalConfigJsonBytes, err := raftInternalConfig.ToJSONBytes()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create raft node config")
	}
	err = liveconf.Publish(raftInternalConfig.GetConfigKey(), raftInternalConfigJsonBytes)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to publis raft node config")
	}

	select {}
}
