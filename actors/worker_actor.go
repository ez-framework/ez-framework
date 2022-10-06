package actors

import (
	"encoding/json"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"

	"github.com/ez-framework/ez-framework/http_helpers"
)

// upgrader is the setting passed when we upgrade the websocket HTTP connection.
// We don't buffer so that downstreams can get config very quickly.
var upgrader = websocket.Upgrader{
	ReadBufferSize:  0,
	WriteBufferSize: 0,
}

// NewWorkerActor is the constructor for WorkerActor
func NewWorkerActor(actorConfig ActorConfig, name string) (*WorkerActor, error) {
	name = "ez-worker-" + name

	actor := &WorkerActor{
		Actor: Actor{
			config:     actorConfig,
			streamName: name,
			ConfigKV:   actorConfig.ConfigKV,
		},
	}

	// WorkerActor must use nats.WorkQueuePolicy.
	// We want the queueing behavior where a message is popped 1 by 1 by 1 random worker.
	if actor.config.Nats.StreamConfig.Retention != nats.WorkQueuePolicy {
		actor.config.Nats.StreamConfig.Retention = nats.WorkQueuePolicy
	}

	err := actor.setupConstructor()
	if err != nil {
		return nil, err
	}

	return actor, nil
}

// WorkerActor is a generic Actor.
// When it received an UPDATE command, it will execute the comand with the payload as parameters.
// DELETE is a no-op because WorkerActor doesn't store its config in the KV store.
type WorkerActor struct {
	Actor
}

// WSHandler is a websocket HTTP handler.
// It receives websocket connections and then pushes config data to websocket clients.
func (actor *WorkerActor) WSHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		http_helpers.RenderJSONError(actor.log(zerolog.ErrorLevel), w, r, err, http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	actor.log(zerolog.DebugLevel).Msg("received a websocket connection")

	wg := sync.WaitGroup{}

	wg.Add(1)

	sub, err := actor.jsc().ChanQueueSubscribe(actor.subscribeSubjects(), "workers", actor.subscriptionChan)
	if err != nil {
		actor.log(zerolog.ErrorLevel).Err(err).Msg("failed to subscribe to nats subjects")
	}
	defer sub.Unsubscribe()

	go func() {
		defer actor.wg.Done()

		for {
			select {
			case <-r.Context().Done():
				actor.log(zerolog.DebugLevel).
					Bool("ctx.done", true).
					Msg("received exit signal from the user. Exiting for loop")

				return

			case msg := <-actor.subscriptionChan:
				err := msg.AckSync()
				if err != nil {
					actor.log(zerolog.ErrorLevel).Err(err).Msg("failed to ack the message")
					continue
				}

				natsMsgBytes, err := json.Marshal(msg)
				if err != nil {
					actor.log(zerolog.ErrorLevel).Err(err).Msg("failed to marshal message for websocket clients")
					continue
				}

				err = conn.WriteMessage(websocket.TextMessage, natsMsgBytes)
				if err != nil {
					actor.log(zerolog.ErrorLevel).Err(err).Msg("failed to push message to websocket clients")
				}
			}
		}
	}()
	wg.Wait()
}
