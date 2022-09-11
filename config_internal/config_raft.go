package config_internal

import (
	"encoding/json"
)

type INatsAddr interface {
	GetNatsAddr() string
}

type ConfigRaft struct {
	LogPath     string
	ClusterName string
	ClusterSize int
	NatsAddr    string
}

func (cr ConfigRaft) GetNatsAddr() string {
	return cr.NatsAddr
}

func (cr ConfigRaft) ToJSONBytes() ([]byte, error) {
	return json.Marshal(cr)
}

func (cr ConfigRaft) GetConfigKey() string {
	return "ez-configlive.raft-node"
}
