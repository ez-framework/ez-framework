# Easy Framework

NOTE: This framework is still a long way to go, I am still in the exploratory phase.


## Problem Statement

I want some of Erlang's OTP features without looking alien in Go world.

* Each daemon should be as small as a goroutine.

* Each daemon is start and stoppable remotely.

* Each daemon config is persistable in a KV store.

* Each daemon has access to a global pubsub system (Nats).

* The API must be clear and easy to adopt.

* This framework must be heavily tested.


## Development Environment

```
# to start nats
docker compose up
```

## Dev Notes

Commands to start a raft and to start a cron remotely.

```
curl -d '{"ez-raft": {"LogDir":"./.data/","Name":"cluster","Size":3}}' http://localhost:3000/api/admin/configkv

curl -d '{"ID":"1664724638","Timezone":"UTC","Schedule":"* * * * *","WorkerQueue":"hello"}' http://localhost:3000/api/admin/cron

curl -d '{}' http://localhost:3000/api/admin/worker/hello
```

