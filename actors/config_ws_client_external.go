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
	SetOnPutChannels([](chan []byte))
	SetOnDeleteChannels([](chan []byte))
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
