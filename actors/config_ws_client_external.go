package actors

type IPutDelete interface {
	IPut
	IDelete
}

type IPut interface {
	Put(string, []byte) error
}

type IDelete interface {
	Delete(string) error
}

type IConfigWSClientActorSettings interface {
	GetWSURL() string
	GetKV() IPutDelete
	GetOnPutChannels() [](chan []byte)
	GetOnDeleteChannels() [](chan []byte)
}

type ConfigWSClientActorSettings struct {
	WSURL            string
	KV               IPutDelete
	OnPutChannels    [](chan []byte)
	OnDeleteChannels [](chan []byte)
}

func (settings ConfigWSClientActorSettings) GetWSURL() string {
	return settings.WSURL
}

func (settings ConfigWSClientActorSettings) GetKV() IPutDelete {
	return settings.KV
}

func (settings ConfigWSClientActorSettings) GetOnPutChannels() [](chan []byte) {
	return settings.OnPutChannels
}

func (settings ConfigWSClientActorSettings) GetOnDeleteChannels() [](chan []byte) {
	return settings.OnDeleteChannels
}

type IConfigWSActorPayload interface {
	GetMethod() string
	GetBody() map[string]interface{}
}

type ConfigWSActorPayload struct {
	Method string
	Body   map[string]interface{}
}

func (payload ConfigWSActorPayload) GetMethod() string { return payload.Method }

func (payload ConfigWSActorPayload) GetBody() any { return payload.Body }
