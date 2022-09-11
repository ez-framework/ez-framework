package main

import (
	"flag"
	"os"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

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

	raftNode, err := raft.NewRaft(*clusterName, *logPath, *clusterSize, *natsAddr)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create a raft node")
	}

	defer raftNode.Close()

	raftNode.Run()
}
