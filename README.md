# Easy Framework

## Development Environment

```
docker compose up
```

## Misc

```
curl -d '{"ez-raft": {"LogDir":"./.data/","Name":"cluster","Size":3}}' http://localhost:3000/api/admin/configkv

curl -d '{"ID":"1663552007","Timezone":"UTC","Schedule":"* * * * *","WorkerQueue":"ez-worker-hello"}' http://localhost:3000/api/admin/cron
```

## Notes 

The only dependency is Nats.

It can:
* Election: Perform leader election easily. [DONE]

  * See: https://github.com/tylertreat/nats-leader-election/blob/master/main.go

* Election: Easy way to execute functions only as a leader. [DONE]

* Election: Easy way to execute functions only as a follower. [DONE]

* Election: A way to identify if a daemon is a leader or not.

* Config: Easy way to receive messages on every node via pubsub. [DONE]

* Config: Easy way to update config and password via key-value store and HTTP. [DONE]

* Config & IoT+WS: Easy way to receive config update through public websocket endpoint. [DONE]

  * TODO: The client loop needs to handle error and sudden termination. It also have to retry infinitely.

* Jobs: Easy way to schedule cron via leader functionality. [DONE]

* Jobs: Easy way to have round robin worker via Jetstream as queue. [DONE]

* Security: LDAP ready with concepts of groups.

* Security: Custom username+password via basic auth.

* Bullet proofing.

  * Each Actor must stop cleanly if needed to.

  * Tests.

  * Godoc.
