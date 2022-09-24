package actors

import (
	"bytes"
	"fmt"
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

func TestLaunchAndSave(t *testing.T) {
	server, nc, jetstreamContext := newNatsServer(t)
	defer server.Shutdown()

	confkv, err := configkv.NewConfigKV(jetstreamContext)
	if err != nil {
		t.Fatal("failed to setup KV store")
	}

	// ---------------------------------------------------------------------------
	// Example on how to create ConfigActor
	configActorConfig := ActorConfig{
		NatsConn:         nc,
		JetStreamContext: jetstreamContext,
		ConfigKV:         confkv,
		StreamConfig: &nats.StreamConfig{
			MaxAge:    3 * time.Second,
			Retention: nats.WorkQueuePolicy,
		},
	}

	configActor, err := NewConfigActor(configActorConfig)
	if err != nil {
		t.Fatal("failed to create ConfigActor")
	}

	// Sending a config payload to be stored in KV store.
	// In the real world, we are using the RunSubscriberAsync() to execute changes
	// and Publish() to send update.
	// Example: Publish(ez-config.command:POST)
	//          Payload: {"ez-raft": {"LogDir":"./.data/","Name":"cluster","Size":3}}
	// But for testing, we'll do it synchronously.

	raftConfigPayload := []byte(`{"LogDir":"./.data/","Name":"cluster","Size":3}`)
	payload := []byte(fmt.Sprintf(`{"ez-raft": %s}`, raftConfigPayload))

	msg := &nats.Msg{
		Subject: "ez-config.command:POST",
		Data:    payload,
	}

	configActor.RunSubscriberSync(msg)

	// Check if we saved the config.
	inBytes, err := confkv.GetConfigBytes("ez-raft")
	if err != nil {
		t.Fatal("failed to fetch config from KV store")
	}
	defer confkv.KV.Delete("ez-raft")

	if !bytes.Equal(inBytes, raftConfigPayload) {
		t.Fatalf("did not save the config correctly. Got: %s", inBytes)
	}
}
