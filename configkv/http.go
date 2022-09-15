package configkv

import (
	"fmt"
	"net/http"

	"github.com/go-chi/render"
	"github.com/nats-io/nats.go"
)

func NewConfigKVHTTPGetAll(configkv *ConfigKV) *ConfigKVHTTPGetAll {
	return &ConfigKVHTTPGetAll{configkv: configkv}
}

type ConfigKVHTTPGetAll struct {
	configkv *ConfigKV
}

func (handler *ConfigKVHTTPGetAll) kv() nats.KeyValue {
	return handler.configkv.KV
}

func (handler *ConfigKVHTTPGetAll) renderJSONError(w http.ResponseWriter, r *http.Request, err error) {
	content := make(map[string]error)
	content["error"] = err
	w.WriteHeader(500)
	render.JSON(w, r, content)
}

func (handler *ConfigKVHTTPGetAll) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	keys, err := handler.kv().Keys()
	if err != nil {
		configKVLogger.Error().Err(err).Msg("Failed to render KV content")
		handler.renderJSONError(w, r, err)
		return
	}

	content := "{"

	for _, key := range keys {
		configBytes, err := handler.configkv.GetConfigBytes(key)
		if err != nil {
			configKVLogger.Error().Err(err).Str("key", key).Msg("Failed to render KV content")
			handler.renderJSONError(w, r, err)
		}

		content += fmt.Sprintf(`"%s": `, key) + string(configBytes)
	}

	content += "}"

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write([]byte(content))
}
