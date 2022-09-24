package actors

import (
	"bytes"
	"testing"
	"time"

	"github.com/ez-framework/ez-framework/configkv"
	"github.com/nats-io/nats.go"
)

func TestLaunchAndSave(t *testing.T) {
	server, nc, jetstreamContext := newNatsServer(t)
	defer server.Shutdown()

	confkv, err := configkv.NewConfigKV(jetstreamContext)
	if err != nil {
		t.Fatal("failed to setup KV store")
	}

	// ---------------------------------------------------------------------------
	// common configuration for all actors
	globalActorConfig := ActorConfig{
		NatsConn:         nc,
		JetStreamContext: jetstreamContext,
		ConfigKV:         confkv,
	}

	// ---------------------------------------------------------------------------
	// Example on how to create ConfigActor
	configActorConfig := globalActorConfig
	configActorConfig.StreamConfig = &nats.StreamConfig{
		MaxAge:    1 * time.Minute,
		Retention: nats.WorkQueuePolicy,
	}

	configActor, err := NewConfigActor(configActorConfig)
	if err != nil {
		t.Fatal("failed to create ConfigActor")
	}
	go configActor.RunSubscriber()

	// Example: Publish(ez-config.command:POST)
	//          Payload: {"ez-raft": {"LogDir":"./.data/","Name":"cluster","Size":3}}
	configActor.Publish("ez-config.command:POST", []byte(`{"ez-raft": {"LogDir":"./.data/","Name":"cluster","Size":3}}`))

	// Check if we saved the config.
	inBytes, err := confkv.GetConfigBytes("ez-raft")
	if err != nil {
		t.Fatal("failed to fetch config from KV store")
	}

	if !bytes.Equal(inBytes, []byte(`{"LogDir":"./.data/","Name":"cluster","Size":3}`)) {
		t.Fatal("did not save the config correctly")
	}
}
