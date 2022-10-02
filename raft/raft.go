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
	"github.com/rs/zerolog/diode"
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

		NatsAddr:            conf.NatsAddr,
		HTTPAddr:            conf.HTTPAddr,
		ExpectedClusterSize: conf.Size,
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

	outWriter := diode.NewWriter(os.Stdout, 1000, 0, nil)
	errWriter := diode.NewWriter(os.Stderr, 1000, 0, nil)

	r.infoLogger = zerolog.New(zerolog.ConsoleWriter{Out: outWriter, TimeFormat: time.RFC3339}).With().Timestamp().Logger()
	r.errorLogger = zerolog.New(zerolog.ConsoleWriter{Out: errWriter, TimeFormat: time.RFC3339}).With().Timestamp().Logger()
	r.debugLogger = zerolog.New(zerolog.ConsoleWriter{Out: errWriter, TimeFormat: time.RFC3339}).With().Timestamp().Logger()

	return r, nil
}

// Raft is a structure that represents a Raft node
type Raft struct {
	Name                string
	Node                *graft.Node
	LogDir              string
	ExpectedClusterSize int
	NatsAddr            string
	HTTPAddr            string

	ExitChan        chan bool
	ErrChan         chan error
	StateChangeChan chan graft.StateChange

	OnBecomingLeader    func(state graft.State)
	OnBecomingFollower  func(state graft.State)
	OnBecomingCandidate func(state graft.State)
	OnClosed            func(state graft.State)

	clusterInfo graft.ClusterInfo
	infoLogger  zerolog.Logger
	errorLogger zerolog.Logger
	debugLogger zerolog.Logger
}

func (r *Raft) log(lvl zerolog.Level) *zerolog.Event {
	switch lvl {
	case zerolog.ErrorLevel:
		return r.errorLogger.Error().
			Str("raft.cluster.name", r.Name).
			Int("raft.cluster.expected-size", r.ExpectedClusterSize).
			Str("raft.nats.addr", r.NatsAddr).
			Str("raft.log.dir", r.LogDir)

	case zerolog.DebugLevel:
		return r.debugLogger.Debug().
			Str("raft.cluster.name", r.Name).
			Int("raft.cluster.expected-size", r.ExpectedClusterSize).
			Str("raft.nats.addr", r.NatsAddr).
			Str("raft.log.dir", r.LogDir)
	default:
		return r.infoLogger.Info().
			Str("raft.cluster.name", r.Name).
			Int("raft.cluster.expected-size", r.ExpectedClusterSize).
			Str("raft.nats.addr", r.NatsAddr).
			Str("raft.log.dir", r.LogDir)
	}
}

// handleState handles the changing of Raft node's state
func (r *Raft) handleState(state graft.State) {
	switch state {
	case graft.LEADER:
		r.log(zerolog.DebugLevel).Msg("becoming leader")

		if r.OnBecomingLeader != nil {
			r.OnBecomingLeader(state)
		}

	case graft.FOLLOWER:
		r.log(zerolog.DebugLevel).Msg("becoming follower")

		if r.OnBecomingFollower != nil {
			r.OnBecomingFollower(state)
		}

	case graft.CANDIDATE:
		r.log(zerolog.DebugLevel).Msg("becoming candidate")

		if r.OnBecomingCandidate != nil {
			r.OnBecomingCandidate(state)
		}

	case graft.CLOSED:
		r.log(zerolog.DebugLevel).Msg("raft is closed")

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
				r.log(zerolog.DebugLevel).Msg("received signal from <-ctx.Done")
				return

			case <-r.ExitChan:
				r.log(zerolog.DebugLevel).Msg("received signal from <-r.ExitChan")
				return

			case change := <-r.StateChangeChan:
				r.handleState(change.To)

			case err := <-r.ErrChan:
				r.log(zerolog.ErrorLevel).Caller().Err(err).Msg("Received an error")
				return
			}
		}
	}()

	wg.Wait()
}

// Close stops participating in quorum election.
func (r *Raft) Close() {
	r.ExitChan <- true
	close(r.ExitChan)
	close(r.StateChangeChan)
	close(r.ErrChan)
	r.Node.Close()
}
