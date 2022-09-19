package actors

import (
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"

	"github.com/ez-framework/ez-framework/configkv"
)

// Actor is the base struct for all actors.
// It provides common helper functions.
type Actor struct {
	actorConfig ActorConfig
	jc          nats.JetStreamContext
	streamName  string
	ConfigKV    *configkv.ConfigKV
	infoLogger  *zerolog.Event
	errorLogger *zerolog.Event
	debugLogger *zerolog.Event
}

// setupStream creates a dedicated stream for this actor
func (actor *Actor) setupStream(streamConfig *nats.StreamConfig) error {
	actor.infoLogger.
		Str("stream.name", actor.streamName).
		Msg("about to setup a new stream")

	streamConfig.Name = actor.streamName
	streamConfig.Subjects = append(streamConfig.Subjects, actor.streamName+".>")

	_, err := actor.jc.AddStream(streamConfig)
	if err != nil {
		actor.errorLogger.Err(err).
			Str("stream.name", actor.streamName).
			Msg("failed to create or get a stream")

		return err
	}

	return nil
}

// subscribeSubjects is usually the streamName followed by any matching characters.
// Example: stream-name.>. The greater than symbol means match more than one subpaths.
// NATS subpaths are delimited with dots.
func (actor *Actor) subscribeSubjects() string {
	return actor.streamName + ".>"
}

// kv gets the underlying KV store
func (actor *Actor) kv() nats.KeyValue {
	return actor.ConfigKV.KV
}

// keyWithCommand appends the command at the end of the key.
// The nats key looks like this: stream-name.optional-key.command:POST|PUT|DELETE.
func (actor *Actor) keyWithCommand(key, command string) string {
	return key + ".command:" + command
}

// keyWithoutCommand strips the command which is appended at the end.
// The nats key looks like this: stream-name.optional-key.command:POST|PUT|DELETE.
func (actor *Actor) keyWithoutCommand(key string) string {
	keyEndIndex := strings.Index(key, ".command:")
	return key[0:keyEndIndex]
}

// keyHasCommand checks if the nats key has a command.
// The nats key looks like this: stream-name.optional-key.command:POST|PUT|DELETE.
func (actor *Actor) keyHasCommand(key, command string) bool {
	return strings.HasSuffix(key, ".command:"+command)
}

// Publish data into JetStream with a nats key.
// The nats key looks like this: stream-name.optional-key.command:POST|PUT|DELETE.
func (actor *Actor) Publish(key string, data []byte) error {
	_, err := actor.jc.Publish(key, data)
	if err != nil {
		actor.errorLogger.Err(err).
			Str("publish.key", key).
			Msg("failed to publish to JetStream")
	}

	return err
}
