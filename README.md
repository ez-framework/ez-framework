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

6. Config: Easy way to receive config update through public websocket endpoint.

7. IoT+WS: Easy way to scale websocket via pubsub.

8. IoT+WS: Easy way to send config to IoT edge.

9. Jobs: Easy way to schedule cron via leader functionality.

10. Jobs: Easy way to have round robin worker via Jetstream as queue.

11. Security: LDAP ready with concepts of groups.

12. Security: Custom username+password via basic auth.
