# Easy Framework

## Development Environment

```
docker compose up
```

## Misc

```
curl -d '{"ez-raft": {"LogDir":"./.data/","Name":"cluster","Size":3}}' http://localhost:3000/api/admin/configkv

curl -d '{"ID":"1664724638","Timezone":"UTC","Schedule":"* * * * *","WorkerQueue":"hello"}' http://localhost:3000/api/admin/cron
```

## Notes 

The only dependency is Nats.

* Bullet proofing.

  * Each Actor must stop cleanly if needed to.

  * Tests.

  * Godoc.


## There are a number of people trying to solve this in 1 runtime.

They are more or less similar to errgroup.

They served as an inspiration on how to streamline the interface. (Which we have a long way to go).

* https://github.com/autom8ter/machine

* https://pkg.go.dev/github.com/oklog/run#pkg-overview

  * https://github.com/oklog/run/blob/v1.1.0/actors.go

  * https://github.com/oklog/run/blob/v1.1.0/group.go

  * https://github.com/stephenafamo/orchestra/blob/master/server.go


## Problems

1. Not enough HTTP VERB

  * We need explicit UNSUB command.

    * We need it to call wg.Done()

