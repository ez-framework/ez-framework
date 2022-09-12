package config_internal

type IConfigInternal interface {
	INatsAddr
	ToJSONBytes() ([]byte, error)
	GetConfigKey() string
}

var JetStreamStreamName string = "ez-configlive"
var JetStreamStreamSubjects string = JetStreamStreamName + ".*"
var KVBucketName string = JetStreamStreamName
