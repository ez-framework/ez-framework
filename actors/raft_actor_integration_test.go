package actors

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/ez-framework/ez-framework/configkv"
)

func init() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
}

func TestSaveAndDeleteConfig(t *testing.T) {
	server, nc, jetstreamContext := newNatsServer(t)
	defer server.Shutdown()

	confkv, err := configkv.NewConfigKV(jetstreamContext)
	if err != nil {
		t.Fatal("failed to setup KV store")
	}

	// ---------------------------------------------------------------------------

	raftActorConfig := ActorConfig{
		NatsAddr:         nats.DefaultURL,
		HTTPAddr:         ":3000",
		NatsConn:         nc,
		JetStreamContext: jetstreamContext,
		ConfigKV:         confkv,
		StreamConfig: &nats.StreamConfig{
			MaxAge: 1 * time.Minute,
		},
	}

	raftActor, err := NewRaftActor(raftActorConfig)
	if err != nil {
		t.Fatal("failed to create ConfigActor")
	}

	// Sending a config payload to be stored in KV store.
	// In the real world, we are using the RunConfigListener() to execute changes
	// and PublishConfig() to send update.
	// Example: PublishConfig(ez-raft.command:POST)
	//          Payload: {"LogDir":"./.data/","Name":"cluster","Size":3}
	// But for testing, we'll do it synchronously.

	raftConfigPayload := []byte(`{"LogDir":"./.data/","Name":"cluster","Size":3}`)

	raftActor.configPut(raftActor.streamName, raftConfigPayload)

	// Check if we saved the config.
	inBytes, err := confkv.GetConfigBytes("ez-raft")
	if err != nil {
		t.Fatal("failed to fetch config from KV store")
	}

	if !bytes.Equal(inBytes, raftConfigPayload) {
		t.Fatalf("did not save the config correctly. Got: %s", inBytes)
	}

	err = raftActor.configDelete(raftActor.streamName)
	if err != nil {
		t.Fatal("failed to delete config on KV store")
	}

	_, err = confkv.GetConfigBytes("ez-raft")
	if err == nil {
		t.Fatal("config should have been gone")
	}
}
