package actors

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"

	"github.com/ez-framework/ez-framework/configkv"
	"github.com/ez-framework/ez-framework/http_helpers"
)

type Actor struct {
	jc            nats.JetStreamContext
	jetstreamName string
	ConfigKV      *configkv.ConfigKV
	infoLogger    *zerolog.Event
	errorLogger   *zerolog.Event
}

func (actor *Actor) loggerEvent(logEvent *zerolog.Event) *zerolog.Event {
	return logEvent.
		Str("stream.name", actor.jetstreamName).
		Str("stream.subjects", actor.jetstreamSubjects())
}

func (actor *Actor) infoLoggerEvent() *zerolog.Event {
	return actor.loggerEvent(actor.infoLogger)
}

func (actor *Actor) errLoggerEvent(err error) *zerolog.Event {
	return actor.loggerEvent(actor.errorLogger).Err(err)
}

func (actor *Actor) setupJetStreamStream() error {
	_, err := actor.jc.AddStream(&nats.StreamConfig{
		Name:     actor.jetstreamName,
		Subjects: []string{actor.jetstreamSubjects()},
	})
	if err != nil {
		actor.errLoggerEvent(err).
			Msg("failed to create a new stream")

		return err
	}

	actor.infoLoggerEvent().Msg("created a new stream")
	return nil
}

func (configactor *Actor) kv() nats.KeyValue {
	return configactor.ConfigKV.KV
}

func (actor *Actor) jetstreamSubjects() string {
	return actor.jetstreamName + ".>"
}

// keyWithoutCommand strips the command which is appended at the end
func (actor *Actor) keyWithoutCommand(key string) string {
	keyEndIndex := strings.Index(key, ".command:")
	return key[0:keyEndIndex]
}

func (actor *Actor) keyWithCommand(key, command string) string {
	return key + ".command:" + command
}

func (actor *Actor) keyHasCommand(key, command string) bool {
	return strings.HasSuffix(key, ".command:"+command)
}

// ServeHTTP supports updating and deleting via HTTP.
// Actor's HTTP handler always support only POST, PUT, and DELETE
// HTTP GET should only be supported by the underlying struct.
func (actor *Actor) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	content := make(map[string]interface{})

	err := json.NewDecoder(r.Body).Decode(&content)
	if err != nil {
		http_helpers.RenderJSONError(actor.errorLogger, w, r, err, http.StatusInternalServerError)
		return
	}

	for key, value := range content {
		if !strings.HasPrefix(key, actor.jetstreamName+".") {
			key = actor.jetstreamName + "." + key
		}

		valueJSONBytes, err := json.Marshal(value)
		if err != nil {
			http_helpers.RenderJSONError(actor.errorLogger, w, r, err, http.StatusInternalServerError)
			return
		}

		err = actor.Publish(actor.keyWithCommand(key, r.Method), valueJSONBytes)
		if err != nil {
			http_helpers.RenderJSONError(actor.errorLogger, w, r, err, http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Write([]byte(`{"status":"success"}`))
	}
}

// Publish a new config by passing it into JetStream with configKey identifier
func (actor *Actor) Publish(key string, data []byte) error {
	_, err := actor.jc.Publish(key, data)
	if err != nil {
		actor.errLoggerEvent(err).
			Str("publish.key", key).
			Msg("failed to publish to JetStream")
	}

	return err
}
