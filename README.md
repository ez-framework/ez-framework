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


## Problems

1. Not enough HTTP VERB

  * We need explicit UNSUB command.

    * We need it to call wg.Done()

