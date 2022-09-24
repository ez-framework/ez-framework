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

* Bullet proofing.

  * Each Actor must stop cleanly if needed to.

    * DELETE command must work for all actor to listen to. And it should respond by stopping the subscription.

  * Tests.

  * Godoc.

* Security: LDAP ready with concepts of groups.

* Security: Custom username+password via basic auth.
