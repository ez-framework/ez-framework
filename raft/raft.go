package raft

import (
	"path/filepath"
	"strings"

	"github.com/nats-io/graft"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
)

type ConfigRaft struct {
	LogDir   string
	Name     string
	Size     int
	NatsAddr string
	HTTPAddr string
}

// NewRaft creates a new Raft node
func NewRaft(conf ConfigRaft) (*Raft, error) {
	if conf.Size == 0 {
		conf.Size = 3
	}
	if conf.NatsAddr == "" {
		conf.NatsAddr = nats.DefaultURL
	}

	r := &Raft{
		Name:                conf.Name,
		LogDir:              conf.LogDir,
		ExpectedClusterSize: conf.Size,
		NatsAddr:            conf.NatsAddr,
		HTTPAddr:            conf.HTTPAddr,
	}

	r.ErrChan = make(chan error)
	r.StateChangeChan = make(chan graft.StateChange)

	natsOptions := &nats.DefaultOptions
	natsOptions.Url = conf.NatsAddr

	rpc, err := graft.NewNatsRpc(natsOptions)
	if err != nil {
		return nil, err
	}

	handler := graft.NewChanHandler(r.StateChangeChan, r.ErrChan)
	r.clusterInfo = graft.ClusterInfo{Name: conf.Name, Size: conf.Size}

	// Construct log path
	logFilename := conf.HTTPAddr
	if strings.HasPrefix(logFilename, ":") {
		logFilename = "localhost" + logFilename + ".graft.log"
	}

	logPath := filepath.Join(r.LogDir, logFilename)

	r.Node, err = graft.New(r.clusterInfo, handler, rpc, logPath)
	if err != nil {
		return nil, err
	}

	return r, nil
}

// Raft is a structure that represents a Raft  node
type Raft struct {
	Name                string
	LogDir              string
	ExpectedClusterSize int
	NatsAddr            string
	HTTPAddr            string

	ErrChan             chan error
	StateChangeChan     chan graft.StateChange
	Node                *graft.Node
	OnBecomingLeader    func(state graft.State)
	OnBecomingFollower  func(state graft.State)
	OnBecomingCandidate func(state graft.State)
	OnClosed            func(state graft.State)

	clusterInfo graft.ClusterInfo
}

// handleState handles the changing of Raft node's state
func (r *Raft) handleState(state graft.State) {
	logger := log.Info().
		Str("NatsAddr", r.NatsAddr).
		Str("ClusterName", r.Name).
		Str("LogDir", r.LogDir).
		Int("ExpectedClusterSize", r.ExpectedClusterSize)

	switch state {
	case graft.LEADER:
		logger.Msg("becoming leader")

		if r.OnBecomingLeader != nil {
			r.OnBecomingLeader(state)
		}

	case graft.FOLLOWER:
		logger.Msg("becoming follower")

		if r.OnBecomingFollower != nil {
			r.OnBecomingFollower(state)
		}

	case graft.CANDIDATE:
		logger.Msg("becoming candidate")

		if r.OnBecomingCandidate != nil {
			r.OnBecomingCandidate(state)
		}

	case graft.CLOSED:
		logger.Msg("Closed")

		if r.OnClosed != nil {
			r.OnClosed(state)
		}
	}
}

// Run initiates the quorum participation of this Raft node
func (r *Raft) RunSubscriberAsync() {
	logger := log.Error().
		Str("NatsAddr", r.NatsAddr).
		Str("ClusterName", r.Name).
		Str("LogDir", r.LogDir).
		Int("ExpectedClusterSize", r.ExpectedClusterSize)

	r.handleState(r.Node.State())

	for {
		select {
		case change := <-r.StateChangeChan:
			r.handleState(change.To)
		case err := <-r.ErrChan:
			logger.Err(err).Str("Method", "RunSubscriberAsync()").Msg("Received an error")
		}
	}
}

// Close stops participating in quorum election.
func (r *Raft) Close() {
	r.Node.Close()
	close(r.StateChangeChan)
	close(r.ErrChan)
}
