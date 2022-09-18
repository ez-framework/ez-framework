# Easy Framework

## Development Environment

```
docker compose up
```

## Misc

```
curl -d '{"ez-raft": {"LogDir":"./.data/","Name":"cluster","Size":3}}' http://localhost:3000/api/admin/configkv
```

## Notes 

The only dependency is Nats.

It can:
1. Election: Perform leader election easily. [DONE]

  * See: https://github.com/tylertreat/nats-leader-election/blob/master/main.go

2. Election: Easy way to execute functions only as a leader. [DONE]

3. Election: Easy way to execute functions only as a follower. [DONE]

4. Config: Easy way to receive messages on every node via pubsub. [DONE]

5. Config: Easy way to update config and password via key-value store and HTTP. [DONE]

6. Config & IoT+WS: Easy way to receive config update through public websocket endpoint.

  * Server [Done]

  * Client

7. IoT+WS: Easy way to scale websocket via pubsub. [WONT DO]

  * Give this to customer to implement, they have everything they need including Nats.

8. Jobs: Easy way to schedule cron via leader functionality.

9. Jobs: Easy way to have round robin worker via Jetstream as queue.

10. Security: LDAP ready with concepts of groups.

11. Security: Custom username+password via basic auth.
