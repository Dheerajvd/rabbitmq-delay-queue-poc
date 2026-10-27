# RabbitMQ delay queue POC

A job is published into one of three RabbitMQ queues. The queue TTL holds it for 1, 2, or 5 minutes. When that TTL expires, RabbitMQ dead-letters the message to `poc.ready`, and a Go consumer processes it.

The Go process does not sleep, poll, or schedule the wait. The delay is `x-message-ttl` plus a dead-letter exchange. Simulated processing runs only after the consumer receives the message. It is not the delay.

SQLite stores the lifecycle. A React dashboard shows it, and Server-Sent Events refresh the page while a job is in flight. Timestamps come from the Go backend or from RabbitMQ `x-death`.

## Architecture

Four durable classic queues and one durable direct exchange:

```text
publisher --default exchange--> poc.delay.1m   TTL 60000 ms
                            \-> poc.delay.2m   TTL 120000 ms
                             -> poc.delay.5m   TTL 300000 ms
                                 each dead-letters to poc.ready.exchange
                                 routing key "ready"
                                        |
                                        | reason "expired"
                                        v
                               poc.ready.exchange  --ready-->  poc.ready
                                                                  |
                                                                  v
                                                            Go consumer
                                                            manual ack
```

```mermaid
sequenceDiagram
    participant FE as Frontend
    participant API as Go API
    participant DB as SQLite
    participant DQ as poc.delay.1m / 2m / 5m
    participant EX as poc.ready.exchange
    participant RQ as poc.ready
    participant W as Go consumer

    FE->>API: POST /api/jobs
    API->>DB: PUBLISH_ATTEMPTED
    API->>DQ: persistent publish via default exchange
    DQ-->>API: publisher confirm
    API->>DB: PUBLISH_CONFIRMED / WAITING
    API-->>FE: 201 and SSE
    Note over DQ: queue TTL 60s, 120s, or 300s
    DQ->>EX: dead-letter expired message
    EX->>RQ: routing key ready
    RQ->>W: basic.deliver
    W->>DB: BROKER_READY and CONSUMPTION_STARTED
    W-->>FE: SSE
    W->>W: simulated processing
    W->>DB: CONSUMPTION_COMPLETED
    W->>RQ: basic.ack
    W-->>FE: SSE
```

Nothing in a request creates a queue. Per-message `expiration` is not set, so the queue TTL is the only delay. The delay queues have no consumers. A consumer on a TTL queue would receive the message immediately.

Classic queues are declared with `x-queue-type=classic`. RabbitMQ prefers policies over hardcoded `x-arguments` because arguments cannot be changed without deleting the queue. This POC still sets `x-message-ttl`, `x-dead-letter-exchange`, and `x-dead-letter-routing-key` on each delay queue.

Quorum queues can dead-letter with internal publisher confirms. Classic dead-lettering does not, so a broker crash during the hop can drop a message. Classic queues are used so the TTL behavior matches the queue-argument examples.

| Name | Role |
| --- | --- |
| `poc.delay.1m` | TTL 60000 ms. Dead-letters to `poc.ready.exchange` with routing key `ready`. |
| `poc.delay.2m` | TTL 120000 ms. Same dead-letter route. |
| `poc.delay.5m` | TTL 300000 ms. Same dead-letter route. |
| `poc.ready` | Bound to that exchange. The consumer reads it with manual acknowledgements. |

`poc.ready.exchange` is a direct exchange, not a queue. Publishing uses the default exchange. The routing key is the chosen delay queue name.

## How a job moves

1. The dashboard or `POST /api/jobs` chooses 1, 2, or 5 minutes. Omitting `delay_minutes` uses 5. Any other value is HTTP 400.
2. `published_at` is taken in UTC. `expected_ready_at` is that time plus the selected minutes. This is an estimate for the countdown. RabbitMQ does not read it.
3. SQLite stores the job and a `PUBLISH_ATTEMPTED` event before the publish.
4. The API publishes persistent JSON to the matching delay queue and waits for a publisher confirm.
5. A positive confirm stores `publish_confirmed_at` and sets the status to `WAITING`.
6. The message sits in that delay queue until the queue TTL expires. No application timer is involved.
7. RabbitMQ dead-letters it to `poc.ready`.
8. The consumer reads `x-death` for that job's delay queue and reason `expired`. It stores `broker_ready_at` from that header, then `consumption_started_at`.
9. It sleeps for the job's processing duration, default 2 seconds, allowed range 0–30. Then it stores `consumption_completed_at` and acknowledges the delivery.

The confirm means the broker accepted the publish. It does not mean a consumer has seen the message.

Statuses: `PUBLISH_ATTEMPTED`, `WAITING`, `PROCESSING`, `COMPLETED`, `PUBLISH_FAILED`, `CONSUMPTION_FAILED`.

Events: `PUBLISH_ATTEMPTED`, `PUBLISH_CONFIRMED`, `PUBLISH_FAILED`, `BROKER_READY`, `CONSUMPTION_STARTED`, `CONSUMPTION_COMPLETED`, `CONSUMPTION_FAILED`.

### Time

| Field | Who sets it | Meaning |
| --- | --- | --- |
| `published_at` | API, just before publish | Clock start. Application time, not a broker time. |
| `publish_confirmed_at` | API, after a positive confirm | Broker accepted the publish. Stays null on failure. |
| `expected_ready_at` | API: `published_at` plus 1, 2, or 5 minutes | Countdown only. Reaching zero does not change status. |
| Queue TTL | The delay queue: 60s, 120s, or 300s | The real wait. |
| `broker_ready_at` | `x-death.time` for that delay queue, reason `expired` | When RabbitMQ dead-lettered it. Second resolution. Null, with `broker_warning`, when the header is missing or does not match. |
| `consumption_started_at` | Consumer, on receipt from `poc.ready` | Delay is over. A redelivery does not overwrite it. |
| Processing seconds | Consumer sleep, 0–30 | Fake work after delivery. It does not change the TTL. |
| `consumption_completed_at` | Consumer, then ack | Job done. |

Stored timestamps are UTC RFC 3339 with nanoseconds when the source has them. The UI shows local time and keeps the UTC string.

`x-death.time` is the first dead-letter time for that queue and reason. Classic queues dead-letter an expired message when it reaches the head, which can be slightly after the TTL. The server does not invent a broker time from the consumer clock.

### Consumer

One consumer, tag `poc-worker`, prefetch 1, manual ack, on `poc.ready` only. It reconnects after broker errors. Shutdown requeues the in-flight delivery.

Processing errors nack and requeue up to three times in this process. After that the job becomes `CONSUMPTION_FAILED` and the delivery is nacked without requeue. `poc.ready` has no dead-letter exchange, so RabbitMQ drops that message. No failure queue is declared.

A duplicate delivery does not overwrite timestamps or write a second completion. A completed job is acked. This is at-least-once, not exactly-once.

SQLite and RabbitMQ are not one transaction:

- The insert succeeds and the publish fails: the row is `PUBLISH_FAILED`. Nothing stays in the queue. There is no automatic republish.
- The publish is confirmed and the SQLite update fails: the message is in the delay queue while the row may still say `PUBLISH_ATTEMPTED`. The consumer may move that row to `PROCESSING` when the delivery arrives.
- The confirm wait times out: the broker may still have the message. The API reports failure and leaves `publish_confirmed_at` null.

A transactional outbox is not implemented.

### Dashboard

The delay control is a dropdown: 1 min, 2 min, or 5 min. It starts on 5 min. The countdown subtracts the browser clock from `expected_ready_at`. It never writes status.

**Clear** calls `DELETE /api/jobs` and hard-deletes every job and event in SQLite. It does not purge RabbitMQ. Messages already in a delay queue are still delivered later; the consumer will not find a matching row.

The page has no status filter. `GET /api/jobs?status=WAITING` still filters on the API. Live updates use `GET /api/events` (`hello`, job events, a `cleared` event, and a 15-second heartbeat). REST remains the source of truth after a refresh.

## Prerequisites

- Go 1.24 or newer
- Node.js 22
- Docker with Compose

## Run it

Create a gitignored `.env` in the repository root when the defaults are not enough. The sample is in [Configuration](#configuration). `.env` is not committed.

The server loads `.env` from the working directory, or from the parent directory when it is started in `backend/`. A variable already set in the shell wins. `make` exports `.env` before Go starts, so `GOTOOLCHAIN` applies to `make backend`.

Start RabbitMQ:

```bash
docker compose up -d rabbitmq
```

Management UI: <http://localhost:15672>  
User: `guest`  
Password: `guest`

AMQP: `amqp://guest:guest@localhost:5672/`

If 5672 or 15672 is already in use, set `RABBITMQ_AMQP_PORT` and `RABBITMQ_MANAGEMENT_PORT` in `.env` and point `AMQP_URL` at the AMQP port. Compose reads those port variables. Do not point this POC at another project's broker.

Start the backend from the repository root:

```bash
make backend
```

Or:

```bash
cd backend
go run ./cmd/server
```

The API listens on <http://localhost:8080>.

Start the frontend in another terminal:

```bash
cd frontend
npm install
npm run dev
```

The dashboard is <http://localhost:5173>. Vite proxies `/api` and `/healthz` to the Go server.

Other Make targets:

```bash
make infra-up
make infra-down
make test
make test-integration
```

SQLite defaults to `backend/data/poc.db` when the server is started from `backend/`. History survives browser refreshes and backend restarts. **Clear** removes that history.

## What you should see

1. Choose 1, 2, or 5 minutes and publish. The job is `WAITING`, with `published_at` and `publish_confirmed_at`.
2. The countdown tracks `expected_ready_at`. At zero it says the expected delay elapsed and that the UI is still waiting for RabbitMQ. The status stays `WAITING`.
3. In the management UI the message sits in `poc.delay.1m`, `poc.delay.2m`, or `poc.delay.5m`. Those queues have no consumers. `poc.ready` has one.
4. After the selected TTL, RabbitMQ dead-letters the message through `poc.ready.exchange` into `poc.ready`.
5. The consumer records `broker_ready_at` from `x-death`, then `consumption_started_at`, waits for the simulated processing duration, then records `consumption_completed_at` and acknowledges the delivery.
6. Refresh the browser or restart `go run`. The same timestamps are still there, until **Clear**.

## API

Publish a one-minute job:

```bash
curl -s -X POST http://localhost:8080/api/jobs \
  -H 'Content-Type: application/json' \
  -d '{"delay_minutes":1,"processing_duration_seconds":2,"payload":{"message":"Sample WhatsApp message"}}'
```

`delay_minutes` may be `1`, `2`, or `5`. Omitting it uses 5 and publishes to `poc.delay.5m`. `3`, `0`, and any other value return HTTP 400.

List, newest first. `status` is optional:

```bash
curl -s http://localhost:8080/api/jobs
curl -s 'http://localhost:8080/api/jobs?status=WAITING'
```

One job, including events and derived metrics:

```bash
curl -s http://localhost:8080/api/jobs/JOB_ID
```

Delete every job in SQLite. This does not purge queues:

```bash
curl -s -X DELETE http://localhost:8080/api/jobs
```

Live updates:

```bash
curl -N http://localhost:8080/api/events
```

Health:

```bash
curl -s http://localhost:8080/healthz
```

## Tests

Unit tests do not need RabbitMQ:

```bash
cd backend && go test ./...
```

Integration tests use a real broker, a separate vhost, and queue names `poc.it.delay` / `poc.it.ready`. Short TTLs exist only there. Declaring a production queue name with a non-production TTL is rejected. The production topology test expects the three delay queues plus `poc.ready`, with TTLs 60000, 120000, and 300000.

```bash
make infra-up
make test-integration
```

If the broker is not on 5672 and 15672, export `RABBITMQ_AMQP_PORT` and `RABBITMQ_MANAGEMENT_PORT` first. The tests dial that broker and run `rabbitmqctl` in the `rabbitmq-delay-poc` container.

## Configuration

There is no `.env.example` file. Put local values in `.env`, which `.gitignore` already excludes. The server and `make` read that file. Defaults below match a broker on the standard ports.

```text
GOTOOLCHAIN=local
AMQP_URL=amqp://guest:guest@localhost:5672/
HTTP_ADDR=:8080
SQLITE_PATH=./data/poc.db
PROCESSING_DURATION_SECONDS=2

# Host ports for Docker. Uncomment both and point AMQP_URL at the AMQP port
# when 5672 or 15672 is already in use.
# RABBITMQ_AMQP_PORT=5672
# RABBITMQ_MANAGEMENT_PORT=15672
```

`PROCESSING_DURATION_SECONDS` is the default simulated handler length when a request omits the field. Callers can override it per job, from 0 through 30. It does not change the TTL.

The delay is chosen per job as 1, 2, or 5 minutes. `FIXED_DELAY_MINUTES` is unused.

`RABBITMQ_AMQP_PORT` and `RABBITMQ_MANAGEMENT_PORT` change the host ports Compose publishes. They default to 5672 and 15672.

## Troubleshooting

- Port 5672 or 15672 already in use: set `RABBITMQ_AMQP_PORT`, `RABBITMQ_MANAGEMENT_PORT`, and `AMQP_URL` together.
- Guest connections refused: `docker/rabbitmq.conf` turns off the guest loopback check so the published Docker port works. That file is for this local POC only.
- `inequivalent arg` when declaring a delay queue: that queue already exists with different arguments. Delete it in the management UI and start the backend again. TTL arguments cannot be edited in place. An old `poc.delay` queue from the first version of this POC is not declared anymore and can be deleted when it is empty.
- Publish returns 502: RabbitMQ was down, or the confirm was nacked or timed out. The job is stored as `PUBLISH_FAILED` and `publish_confirmed_at` stays null.
- Countdown hits zero while status is still `WAITING`: that is expected. Readiness comes from the consumer, not the clock.
- Do not call RabbitMQ's `aliveness-test` management endpoint. It declares an extra queue.

## Known limitations

- Delivery is at-least-once. Publisher confirms and consumer acks do not make a distributed transaction with SQLite.
- There is a window where SQLite has the job and RabbitMQ does not, or the reverse if a confirm is observed and the following database update fails.
- Classic-queue dead-lettering is not internally confirmed. A broker crash during the hop can drop a message.
- Failed processing is retried up to three times on `poc.ready`, then nacked without requeue. No failure queue is declared, so that message is dropped.
- `x-death.time` is second resolution. It is not proof of the exact instant the message became eligible.
- The UI countdown never writes timestamps or status.
- One consumer process is assumed. Two servers would both compete for `poc.ready`.
- A message in a delay queue cannot be selected by id before its TTL. **Clear** deletes the SQLite row only.
