# RPC Docker example

Example with two processes for RPC with autopaho.

'req' continuously sends requests to 'resp' for processing.
Even after failed requests or disconnects, 'req' will simply reconnect and send the next request.

- req
  - sends requests and waits for the response (timeout after 5 seconds)
  - continue with the next request after errors or disconnects.
- resp
  - answers requests
  - simulates processing time, responses will be delayed (1..10 seconds)

## Build and run in Docker

```bash
docker compose up --build
```

## Simulating Network Connection Loss

You can simulate the loss of network connectivity by disconnecting the network adapter within a container. e.g.

```bash
docker network disconnect rpc-docker_paho-test-net req
docker network connect rpc-docker_paho-test-net req
```
