package config_internal

type IConfigInternal interface {
	INatsAddr
	ToJSONBytes() ([]byte, error)
	GetConfigKey() string
}
