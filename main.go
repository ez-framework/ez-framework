package main

import (
	"flag"
	"os"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/didip/easy-framework/liveconfig"
	"github.com/didip/easy-framework/raft"
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
	// Example on how to create a raft node and participate in membership

	raftNode, err := raft.NewRaft(*clusterName, *logPath, *clusterSize, *natsAddr)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create a raft node")
	}
	defer raftNode.Close()

	raftNode.Run()

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

	liveconf, err := liveconfig.NewLiveConfig(jetstreamContext)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create liveconfig")
	}
	liveconf.SubscribeConfigUpdate()
}
