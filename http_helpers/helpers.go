package http_helpers

import (
	"net/http"

	"github.com/go-chi/render"
	"github.com/rs/zerolog"
)

// RenderJSONError is a helper function to show error in JSON response
func RenderJSONError(errLogEvent *zerolog.Event, w http.ResponseWriter, r *http.Request, err error, code int) {
	errLogEvent.Err(err).Msg("failed to respond HTTP request. Rendering JSON error...")

	content := make(map[string]string)
	content["error"] = err.Error()
	w.WriteHeader(code)
	render.JSON(w, r, content)
}
