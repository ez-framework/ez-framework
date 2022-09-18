package configkv

import (
	"fmt"
	"net/http"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"

	"github.com/ez-framework/ez-framework/http_helpers"
)

// NewConfigKVHTTPGetAll is constructor for ConfigKVHTTPGetAll
func NewConfigKVHTTPGetAll(configkv *ConfigKV) *ConfigKVHTTPGetAll {
	return &ConfigKVHTTPGetAll{configkv: configkv}
}

// ConfigKVHTTPGetAll is http handler to render all config in JSON format
type ConfigKVHTTPGetAll struct {
	configkv *ConfigKV
}

func (handler *ConfigKVHTTPGetAll) kv() nats.KeyValue {
	return handler.configkv.KV
}

func (handler *ConfigKVHTTPGetAll) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	keys, err := handler.kv().Keys()
	if err != nil {
		log.Error().Err(err).Str("bucket.name", handler.configkv.bucketName).Msg("failed to render KV content")
		http_helpers.RenderJSONError(log.Error(), w, r, err, http.StatusInternalServerError)
		return
	}

	content := "{"

	for _, key := range keys {
		configBytes, err := handler.configkv.GetConfigBytes(key)
		if err != nil {
			http_helpers.RenderJSONError(log.Error(), w, r, err, http.StatusInternalServerError)
		}

		content += fmt.Sprintf(`"%s": `, key) + string(configBytes)
	}

	content += "}"

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write([]byte(content))
}
