package http_logs

import (
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
)

// HTTPLogsConfig is the config for HTTPLogs
type HTTPLogsConfig struct {
	LogLevel string

	JetStreamContext nats.JetStreamContext

	StreamConfig *nats.StreamConfig
}

// NewHTTPLogs is the constructor for *HTTPLogs
func NewHTTPLogs(conf HTTPLogsConfig) (*HTTPLogs, error) {
	name := "ez-logs"

	httplogs := &HTTPLogs{
		streamName:   name,
		logLevel:     conf.LogLevel,
		jctx:         conf.JetStreamContext,
		streamConfig: conf.StreamConfig,
		logsReceiver: make(chan *nats.Msg),
	}

	httplogs.setupLoggers()

	err := httplogs.setupStream()
	if err != nil {
		httplogs.errorLogger.Caller().Err(err).Msg("failed to setup stream")
		return nil, err
	}

	return httplogs, nil
}

type HTTPLogs struct {
	streamName   string
	logLevel     string
	streamConfig *nats.StreamConfig
	jctx         nats.JetStreamContext
	logsReceiver chan *nats.Msg
	infoLogger   *zerolog.Event
	errorLogger  *zerolog.Event
	debugLogger  *zerolog.Event
}

// setupLoggers
func (httplogs *HTTPLogs) setupLoggers() {
	outLog := zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).With().Timestamp().Logger()
	errLog := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}).With().Timestamp().Logger()
	dbgLog := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}).With().Timestamp().Logger()

	httplogs.infoLogger = outLog.Info().
		Str("stream.name", httplogs.streamName).
		Str("stream.subjects", httplogs.subscribeSubjects())

	httplogs.errorLogger = errLog.Error().
		Str("stream.name", httplogs.streamName).
		Str("stream.subjects", httplogs.subscribeSubjects())

	httplogs.debugLogger = dbgLog.Debug().
		Str("stream.name", httplogs.streamName).
		Str("stream.subjects", httplogs.subscribeSubjects())
}

// subscribeSubjects is usually the streamName followed by any matching characters.
// Example: stream-name.>. The greater than symbol means match more than one subpaths.
// NATS subpaths are delimited with dots.
func (httplogs *HTTPLogs) subscribeSubjects() string {
	return httplogs.streamName + ".>"
}

// setupStream creates a dedicated stream for this actor
func (httplogs *HTTPLogs) setupStream() error {
	if httplogs.streamConfig == nil {
		httplogs.streamConfig = &nats.StreamConfig{}
	}

	httplogs.streamConfig.Name = httplogs.streamName
	httplogs.streamConfig.Subjects = append(httplogs.streamConfig.Subjects, httplogs.subscribeSubjects())

	_, err := httplogs.jc().AddStream(httplogs.streamConfig)
	if err != nil {
		if err.Error() == "nats: stream name already in use" {
			_, err = httplogs.jc().UpdateStream(httplogs.streamConfig)
		} else if err.Error() == "duplicate subjects detected" {
			return nil
		}

		if err != nil {
			httplogs.errorLogger.Caller().Err(err).
				Msg("failed to create or get a stream")

			return err
		}
	}

	return nil
}

// jc gets the JetStreamContext
func (httplogs *HTTPLogs) jc() nats.JetStreamContext {
	return httplogs.jctx
}

func (httplogs *HTTPLogs) Write(data []byte) (int, error) {
	subject := httplogs.streamName + "." + httplogs.logLevel

	_, err := httplogs.jc().PublishAsync(subject, data)
	if err != nil {
		return 0, err
	}

	return len(data), nil
}

// ServeHTTP streams logs to HTTP response
func (httplogs *HTTPLogs) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	loglevel := r.FormValue("loglevel")

	sub, err := httplogs.jc().ChanSubscribe(httplogs.streamName+"."+loglevel, httplogs.logsReceiver)
	if err != nil {
		httplogs.errorLogger.Caller().Err(err).
			Str("loglevel", loglevel).
			Msg("failed to subscribe to stream")

		http.Error(w, "failed to subscribe to stream", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	for {
		select {
		case <-r.Context().Done():
			sub.Unsubscribe()
			return

		case msg := <-httplogs.logsReceiver:
			fmt.Fprintf(w, "%s\n", msg.Data)
			flusher.Flush()
		}
	}
}
