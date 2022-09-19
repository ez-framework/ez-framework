package actors

// IPutDelete is a generic interface to a KV store
type IPutDelete interface {
	IPut
	IDelete
}

// IPut is a generic interface to anything that can save data marked with a key.
type IPut interface {
	Put(string, []byte) error
}

// IDelete is a generic interface to anything that can delete data marked with a key.
type IDelete interface {
	Delete(string) error
}

// IConfigWSClientActorSettings
type IConfigWSClientActorSettings interface {
	GetWSURL() string
	GetKV() IPutDelete
	GetOnPutChannels() [](chan []byte)
	GetOnDeleteChannels() [](chan []byte)
}

// ConfigWSClientActorSettings
type ConfigWSClientActorSettings struct {
	WSURL            string
	KV               IPutDelete
	OnPutChannels    [](chan []byte)
	OnDeleteChannels [](chan []byte)
}

// GetWSURL
func (settings ConfigWSClientActorSettings) GetWSURL() string {
	return settings.WSURL
}

// GetKV
func (settings ConfigWSClientActorSettings) GetKV() IPutDelete {
	return settings.KV
}

// GetOnPutChannels
func (settings ConfigWSClientActorSettings) GetOnPutChannels() [](chan []byte) {
	return settings.OnPutChannels
}

// GetOnDeleteChannels
func (settings ConfigWSClientActorSettings) GetOnDeleteChannels() [](chan []byte) {
	return settings.OnDeleteChannels
}

// IConfigWSActorPayload is the interface to config payload that is sent through a websocket connection.
type IConfigWSActorPayload interface {
	GetMethod() string
	GetBody() map[string]interface{}
}

// ConfigWSActorPayload is the payload that is sent through a websocket connection.
// Think of it as the envelope to the config data.
type ConfigWSActorPayload struct {
	Method string
	Body   map[string]interface{}
}

// GetMethod returns either POST, PUT, or DELETE.
func (payload ConfigWSActorPayload) GetMethod() string { return payload.Method }

// GetBody returns the config data.
func (payload ConfigWSActorPayload) GetBody() any { return payload.Body }
