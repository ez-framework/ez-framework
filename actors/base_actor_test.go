package actors

import (
	"testing"

	nats_server "github.com/nats-io/nats-server/v2/server"
	nats_test "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

type TestActor struct {
	Actor
}

func newNatsServer(t *testing.T) (*nats_server.Server, *nats.Conn, nats.JetStreamContext) {
	opts := &nats_test.DefaultTestOptions
	opts.Port = nats_server.RANDOM_PORT
	opts.JetStream = true

	embeddedServer := nats_test.RunServer(opts)

	serverAddress := embeddedServer.Addr().String()
	connection, err := nats.Connect("nats://"+serverAddress, nats.RetryOnFailedConnect(true))
	if err != nil {
		t.Fatal(err)
	}

	jetstream, err := connection.JetStream()
	if err != nil {
		t.Fatal(err)
	}

	return embeddedServer, connection, jetstream
}

func TestActorsConformToInterface(t *testing.T) {
	funcTest := func(actor IActor) {
		if actor.GetStreamName() != "testing" {
			t.Fatal("TestActor does not implement IActor")
		}
	}

	actor := &TestActor{
		Actor: Actor{
			streamName: "testing",
		},
	}

	actor2 := &ConfigActor{
		Actor: Actor{
			streamName: "testing",
		},
	}

	actor3 := &ConfigWSServerActor{
		Actor: Actor{
			streamName: "testing",
		},
	}

	actor4 := &CronActor{
		Actor: Actor{
			streamName: "testing",
		},
	}

	actor5 := &RaftActor{
		Actor: Actor{
			streamName: "testing",
		},
	}

	actor6 := &WorkerActor{
		Actor: Actor{
			streamName: "testing",
		},
	}

	funcTest(actor)
	funcTest(actor2)
	funcTest(actor3)
	funcTest(actor4)
	funcTest(actor5)
	funcTest(actor6)
}
