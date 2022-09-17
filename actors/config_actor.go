package actors

import (
	"bytes"
	"os"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var configActorLogger = log.With().
	Logger().Output(zerolog.ConsoleWriter{Out: os.Stderr})

// NewConfigActor is the constructor for *ConfigActor
func NewConfigActor(jetstreamContext nats.JetStreamContext) (*ConfigActor, error) {
	configactor := &ConfigActor{
		Actor: Actor{
			jc:            jetstreamContext,
			jetstreamName: "ez-configlive",
			commandChan:   make(chan *nats.Msg),
			infoLogger:    configActorLogger.Info(),
			errorLogger:   configActorLogger.Error(),
		},
	}

	err := configactor.setupJetStreamStream()
	if err != nil {
		return nil, err
	}

	return configactor, nil
}

type ConfigActor struct {
	Actor
}

// updateHandler will be executed inside Run.
func (configactor *ConfigActor) updateHandler(msg *nats.Msg) error {
	configKey := msg.Subject
	configBytes := msg.Data

	// Get existing config
	existingConfig, err := configactor.kv().Get(configactor.keyWithoutCommand(configKey))
	if err == nil {
		// Don't do anything if new config is the same as existing config
		existingConfigBytes := existingConfig.Value()
		if bytes.Equal(existingConfigBytes, configBytes) {
			return nil
		}
	}

	// Update config in kv store
	revision, err := configactor.kv().Put(configactor.keyWithoutCommand(configKey), configBytes)
	if err != nil {
		configactor.errLoggerEvent(err).Msg("failed to update config in KV store")
	} else {
		configactor.infoLoggerEvent().Int64("revision", int64(revision)).Msg("Updated config in KV store")
	}

	return err
}

// deleteHandler will be executed inside Run.
func (configactor *ConfigActor) deleteHandler(msg *nats.Msg) error {
	configKey := msg.Subject

	err := configactor.kv().Delete(configactor.keyWithoutCommand(configKey))
	if err != nil {
		configactor.errLoggerEvent(err).Msg("failed to delete config in KV store")
	}

	return err
}

// Run listens to config changes and update the storage
func (configactor *ConfigActor) Run() {
	subscription := configactor.retrySubscribing(configactor.jetstreamSubjects())
	defer subscription.Unsubscribe()

	configactor.infoLoggerEvent().Msg("Subscribing to nats subjects")

	// Wait until we get a new message
	for {
		msg := <-configactor.commandChan

		configactor.infoLoggerEvent().
			Str("msg.subject", msg.Subject).
			Bytes("msg.data", msg.Data).Msg("Inspecting the content")

		if configactor.keyHasCommand(msg.Subject, "POST") || configactor.keyHasCommand(msg.Subject, "PUT") {
			err := configactor.updateHandler(msg)
			if err != nil {
				configactor.errLoggerEvent(err).
					Caller().
					Err(err).
					Msg("failed to execute updateHandler inside Run()")
			}

		} else if configactor.keyHasCommand(msg.Subject, "DELETE") {
			err := configactor.deleteHandler(msg)
			if err != nil {
				configactor.errLoggerEvent(err).
					Caller().
					Err(err).
					Msg("failed to execute deleteHandler inside Run()")
			}
		}
	}
}
