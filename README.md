# Easy Framework

## Development Environment

```
docker compose up
```

## Notes 

The only dependency is Nats.

It can:
1. Election: Perform leader election easily. [DONE]

  * See: https://github.com/tylertreat/nats-leader-election/blob/master/main.go

2. Election: Easy way to execute functions only as a leader. [DONE]

3. Election: Easy way to execute functions only as a follower. [DONE]

4. Config: Easy way to receive messages on every node via pubsub. [DONE]

5. Config: Easy way to update config and password via key-value store and pubsub to update cache.

  * Need HTTP interface to view config content and to update config. [POC DONE]

6. Config: Easy way to receive config update through public websocket endpoint.

7. Config: in memory cache.

8. IoT+WS: Easy way to scale websocket via pubsub.

9. IoT+WS: Easy way to send config to IoT edge.

10. Jobs: Easy way to schedule cron via leader functionality.

11. Jobs: Easy way to have round robin worker via Jetstream as queue.

12. Security: LDAP ready with concepts of groups.

13. Security: Custom username+password via basic auth.
