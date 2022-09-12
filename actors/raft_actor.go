package actors

import (
	"encoding/json"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"

	"github.com/ez-framework/ez-framework/config_internal"
	"github.com/ez-framework/ez-framework/raft"
)

func NewRaftActor(jetstreamContext nats.JetStreamContext) (*RaftActor, error) {
	ra := &RaftActor{
		jc:         jetstreamContext,
		updateChan: make(chan *nats.Msg),
		configKey:  config_internal.ConfigRaftKey,
	}

	err := ra.setupKVStore()
	if err != nil {
		return nil, err
	}

	return ra, nil
}

type RaftActor struct {
	jc         nats.JetStreamContext
	kv         nats.KeyValue
	configKey  string
	updateChan chan *nats.Msg
	raftNode   *raft.Raft
}

func (ra *RaftActor) setupKVStore() error {
	kv, err := ra.jc.KeyValue(config_internal.KVBucketName)
	if err == nil {
		ra.kv = kv
		return nil
	}

	kv, err = ra.jc.CreateKeyValue(&nats.KeyValueConfig{Bucket: config_internal.KVBucketName})
	if err == nil {
		ra.kv = kv
		return nil
	}

	log.Error().Err(err).Msg("Failed to setup KV store")
	return err
}
func (ra *RaftActor) retrySubscribing(configKey string) *nats.Subscription {
	errLogger := log.Error().Str("configKey", configKey)

	sub, err := ra.jc.ChanSubscribe(configKey, ra.updateChan)
	n := 0
	for err != nil {
		if n > 20 {
			n = 0
		}

		// Log the error and then sleep before subscribing
		errLogger.Err(err).Msg("Failed to subscribe")
		time.Sleep(time.Duration(n*5) * time.Second)

		sub, err = ra.jc.ChanSubscribe(configKey, ra.updateChan)
		n += 1
	}

	return sub
}

func (ra *RaftActor) Run() {
	infoLogger := log.Info().Str("configKey", ra.configKey)
	errLogger := log.Error().Str("configKey", ra.configKey)

	subscription := ra.retrySubscribing(ra.configKey)
	defer subscription.Unsubscribe()

	// Wait until we get a new message
	for {
		msg := <-ra.updateChan

		configBytes := msg.Data

		infoLogger.Bytes("configBytes", configBytes).Msg("Received an update message")
		conf := config_internal.ConfigRaft{}

		err := json.Unmarshal(configBytes, &conf)
		if err != nil {
			errLogger.Err(err).Msg("Failed to unmarshal config")
			continue
		}

		// If there is an existing raftNode, close it.
		if ra.raftNode != nil {
			ra.raftNode.Close()
		}

		raftNode, err := raft.NewRaft(conf.ClusterName, conf.LogPath, conf.ClusterSize, conf.NatsAddr)
		if err != nil {
			errLogger.Err(err).Msg("failed to create a raft node")
		}
		ra.raftNode = raftNode

		infoLogger.Msg("RaftActor is running")
		raftNode.Run()
	}
}

// Publish a new Raft config by passing it into JetStream
func (ra *RaftActor) Publish(configKey string, data []byte) error {
	_, err := ra.jc.Publish(configKey, data)
	if err != nil {
		log.Error().Err(err).Str("configKey", configKey).Msg("Failed to publish config")
	}

	return err
}

func (ra *RaftActor) OnBootLoad() error {
	conf, err := ra.kv.Get(ra.configKey)
	if err != nil {
		log.Error().Err(err).Msg("failed to load raft node config from KV store")
		return err
	}

	raftInternalConfigJSONBytes := conf.Value()

	err = ra.Publish(ra.configKey, raftInternalConfigJSONBytes)
	if err != nil {
		log.Error().Err(err).Msg("failed to publish raft node config")
		return err
	}

	return nil
}
