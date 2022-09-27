package raft

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/graft"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
)

// ConfigRaft is the configuration to spawn a new Raft node
type ConfigRaft struct {
	LogDir   string
	Name     string
	Size     int
	NatsAddr string
	HTTPAddr string
}

// NewRaft is the constructor for a Raft node
func NewRaft(conf ConfigRaft) (*Raft, error) {
	if conf.Size == 0 {
		conf.Size = 3
	}
	if conf.NatsAddr == "" {
		conf.NatsAddr = nats.DefaultURL
	}

	r := &Raft{
		Name:   conf.Name,
		LogDir: conf.LogDir,

		natsAddr:            conf.NatsAddr,
		httpAddr:            conf.HTTPAddr,
		expectedClusterSize: conf.Size,
	}

	r.ErrChan = make(chan error, 64000)
	r.StateChangeChan = make(chan graft.StateChange, 64000)

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

	logPath := filepath.Join(conf.LogDir, logFilename)

	r.Node, err = graft.New(r.clusterInfo, handler, rpc, logPath)
	if err != nil {
		return nil, err
	}

	outLog := zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).With().Timestamp().Logger()
	errLog := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}).With().Timestamp().Logger()

	r.infoLogger = outLog.Info().
		Str("raft.cluster.name", r.Name).
		Int("raft.cluster.expected-size", r.expectedClusterSize).
		Str("raft.nats.addr", r.natsAddr).
		Str("raft.log.dir", conf.LogDir)

	r.errorLogger = errLog.Error().
		Str("raft.cluster.name", r.Name).
		Int("raft.cluster.expected-size", r.expectedClusterSize).
		Str("raft.nats.addr", r.natsAddr).
		Str("raft.log.dir", conf.LogDir)

	r.debugLogger = errLog.Debug().
		Str("raft.cluster.name", r.Name).
		Int("raft.cluster.expected-size", r.expectedClusterSize).
		Str("raft.nats.addr", r.natsAddr).
		Str("raft.log.dir", conf.LogDir)

	return r, nil
}

// Raft is a structure that represents a Raft node
type Raft struct {
	Name   string
	Node   *graft.Node
	LogDir string

	ExitChan        chan bool
	ErrChan         chan error
	StateChangeChan chan graft.StateChange

	OnBecomingLeader    func(state graft.State)
	OnBecomingFollower  func(state graft.State)
	OnBecomingCandidate func(state graft.State)
	OnClosed            func(state graft.State)

	expectedClusterSize int
	natsAddr            string
	httpAddr            string
	clusterInfo         graft.ClusterInfo
	infoLogger          *zerolog.Event
	errorLogger         *zerolog.Event
	debugLogger         *zerolog.Event
}

// handleState handles the changing of Raft node's state
func (r *Raft) handleState(state graft.State) {
	switch state {
	case graft.LEADER:
		r.debugLogger.Msg("becoming leader")

		if r.OnBecomingLeader != nil {
			r.OnBecomingLeader(state)
		}

	case graft.FOLLOWER:
		r.debugLogger.Msg("becoming follower")

		if r.OnBecomingFollower != nil {
			r.OnBecomingFollower(state)
		}

	case graft.CANDIDATE:
		r.debugLogger.Msg("becoming candidate")

		if r.OnBecomingCandidate != nil {
			r.OnBecomingCandidate(state)
		}

	case graft.CLOSED:
		r.debugLogger.Msg("Closed")

		if r.OnClosed != nil {
			r.OnClosed(state)
		}
	}
}

// Run initiates the quorum participation of this Raft node
func (r *Raft) RunBlocking(ctx context.Context) {
	r.handleState(r.Node.State())

	wg := sync.WaitGroup{}

	wg.Add(1)

	go func() {
		defer wg.Done()

		for {
			select {
			case <-ctx.Done():
				r.debugLogger.Msg("received signal from <-r.ExitChan")
				return

			case <-r.ExitChan:
				r.debugLogger.Msg("received signal from <-r.ExitChan")
				return

			case change := <-r.StateChangeChan:
				r.debugLogger.Msg("raft state changed to: " + change.To.String())
				r.handleState(change.To)

			case err := <-r.ErrChan:
				r.errorLogger.Err(err).Caller().Msg("Received an error")
				return
			}
		}
	}()

	wg.Wait()
}

// Close stops participating in quorum election.
func (r *Raft) Close() {
	r.debugLogger.Caller().Msg("(r *Raft) Close() is called")

	r.Node.Close()
	close(r.StateChangeChan)
	close(r.ErrChan)

	r.ExitChan <- true
	close(r.ExitChan)
}
