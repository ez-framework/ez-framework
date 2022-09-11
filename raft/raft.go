package raft

import (
	"github.com/nats-io/graft"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
)

// NewRaft creates a new Raft node
func NewRaft(name, logPath string, expectedClusterSize int, natsAddr string) (*Raft, error) {
	if expectedClusterSize == 0 {
		expectedClusterSize = 3
	}
	if natsAddr == "" {
		natsAddr = nats.DefaultURL
	}

	r := &Raft{
		Name:                name,
		LogPath:             logPath,
		ExpectedClusterSize: expectedClusterSize,
		NatsAddr:            natsAddr,
	}

	r.ErrChan = make(chan error)
	r.StateChangeChan = make(chan graft.StateChange)

	natsOptions := &nats.DefaultOptions
	natsOptions.Url = natsAddr

	rpc, err := graft.NewNatsRpc(natsOptions)
	if err != nil {
		return nil, err
	}

	handler := graft.NewChanHandler(r.StateChangeChan, r.ErrChan)
	r.clusterInfo = graft.ClusterInfo{Name: name, Size: expectedClusterSize}

	r.Node, err = graft.New(r.clusterInfo, handler, rpc, r.LogPath)
	if err != nil {
		return nil, err
	}

	return r, nil
}

// Raft is a structure that represents a Raft  node
type Raft struct {
	Name                string
	LogPath             string
	ExpectedClusterSize int
	NatsAddr            string

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
	logger := log.Info().Str("NatsAddr", r.NatsAddr).Str("ClusterName", r.Name).Str("LogPath", r.LogPath).Int("ExpectedClusterSize", r.ExpectedClusterSize)

	switch state {
	case graft.LEADER:
		logger.Msg("Becoming leader")

		if r.OnBecomingLeader != nil {
			r.OnBecomingLeader(state)
		}

	case graft.FOLLOWER:
		logger.Msg("Becoming follower")

		if r.OnBecomingFollower != nil {
			r.OnBecomingFollower(state)
		}

	case graft.CANDIDATE:
		logger.Msg("Becoming candidate")

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
func (r *Raft) Run() {
	logger := log.Error().Str("NatsAddr", r.NatsAddr).Str("ClusterName", r.Name).Str("LogPath", r.LogPath).Int("ExpectedClusterSize", r.ExpectedClusterSize)

	r.handleState(r.Node.State())

	for {
		select {
		case change := <-r.StateChangeChan:
			r.handleState(change.To)
		case err := <-r.ErrChan:
			logger.Err(err).Str("Method", "Run()").Msg("Received an error")
		}
	}
}

// Close stops participating in quorum election.
func (r *Raft) Close() {
	r.Node.Close()
	close(r.StateChangeChan)
	close(r.ErrChan)
}
