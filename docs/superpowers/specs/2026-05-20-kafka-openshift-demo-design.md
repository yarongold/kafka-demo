# Kafka demo on local OpenShift — design

**Date:** 2026-05-20
**Status:** Approved (brainstorming phase)
**Goal:** A self-contained Kafka demo application that the user can install on their local OpenShift cluster to learn Kafka by doing — producing, consuming, scaling consumer groups, observing partition behavior, and exercising a dead-letter queue.

## Goals & non-goals

**Goals**
- Demonstrate the core Kafka model: producer → topic → consumer.
- Demonstrate consumer-group scaling and partition rebalancing in real time.
- Demonstrate message-key → partition assignment.
- Demonstrate retry + dead-letter-queue (DLQ) behavior on bad records.
- Be installable with a single `oc apply -k` after a one-time image build.
- Provide a visual control plane (Kafka UI) so the user can see topics, partitions, consumer groups, lag, and DLQ records without writing extra code.

**Non-goals (v1)**
- Schema Registry / Avro — JSON on the wire is enough for learning.
- TLS / SCRAM auth between apps and brokers — in-cluster plain listener is fine for a local demo.
- Multi-broker HA — single broker with ephemeral storage; documented in the README as a deliberate simplification.
- Production-grade observability (Prometheus, Grafana) — not needed for the learning goal.
- A custom web UI in the apps themselves — Kafka UI covers visualization; producer/consumer log to stdout.

## Audience & success criteria

The user has a local OpenShift cluster and wants to *learn* Kafka. The demo succeeds if, after installing, the user can:

1. See messages flowing in `oc logs` for both apps within ~30 seconds of install.
2. Open Kafka UI in a browser and see the `orders` and `orders-dlq` topics, their partitions, and a live message stream.
3. Run `oc scale deploy/consumer --replicas=3` and watch the partition assignments rebalance across pods in Kafka UI.
4. See bad records (every 7th message) land on `orders-dlq` with the original exception captured in record headers.

## High-level architecture

```
┌─────────┐   produces    ┌──────────────────┐   consumes   ┌─────────┐
│producer │ ────────────▶ │ orders (3 parts) │ ───────────▶ │consumer │
│ (1 pod) │               └──────────────────┘              │(N pods, │
└─────────┘                        │                        │ same    │
                                   │ bad records            │ group)  │
                                   ▼                        └─────────┘
                          ┌──────────────────┐
                          │  orders-dlq      │
                          └──────────────────┘

                          ┌──────────────────┐
                          │  Kafka UI (Route)│  ← browser
                          └──────────────────┘
```

All components run in a single namespace (default: `kafka-demo`). Strimzi operator is cluster-scoped, installed into `openshift-operators` if not already present.

## Repository layout

```
kafka-demo/
├── pom.xml                         (parent Maven project)
├── common/                         (shared Order POJO + Jackson config)
├── producer/                       (Spring Boot app)
├── consumer/                       (Spring Boot app)
├── k8s/
│   ├── kustomization.yaml          (umbrella)
│   ├── strimzi-operator/           (Subscription to OperatorHub Strimzi)
│   ├── kafka-cluster/              (Kafka CR — 1 broker, ephemeral)
│   ├── topics/                     (KafkaTopic CRs: orders, orders-dlq)
│   ├── apps/                       (producer + consumer Deployments + Services)
│   └── kafka-ui/                   (Provectus Kafka UI Deployment + Route)
├── docs/
│   └── superpowers/specs/2026-05-20-kafka-openshift-demo-design.md  (this file)
└── README.md
```

## Components

### `common` module

A small Maven library jar — depended on by both apps.

- `Order` (Java `record`): `String orderId, String customerId, BigDecimal amount, Instant createdAt`.
- Jackson configuration: `JavaTimeModule` registered, `BigDecimal` serialized as a number (not a string), property naming = camelCase.
- One unit test that round-trips an `Order` through Jackson and asserts equality.

Rationale: keeping the wire model in a shared module prevents the producer and consumer from drifting into incompatible JSON schemas — a common bug source that obscures Kafka learning if it happens.

### `producer` app

Spring Boot 3.x, Java 21, packaged as a fat JAR and containerized via a multi-stage `Dockerfile` (`eclipse-temurin:21-jdk` build stage → `eclipse-temurin:21-jre` runtime, non-root user).

Behavior:
- `@Scheduled(fixedRate = ${PRODUCER_RATE_MS:1000})` emits one order per tick.
- `customerId` drawn from a fixed pool of 10 IDs (`cust-0` … `cust-9`); used as the **Kafka message key** to demonstrate same-key-same-partition.
- Every 7th message gets `amount = BigDecimal.valueOf(-1)` — the "bad" record that the consumer will reject.
- Producer callback logs the assigned partition + offset:
  ```
  sent orderId=ORD-1234 customerId=cust-3 → partition=2 offset=88
  ```
- Spring Actuator exposes `/actuator/health` for liveness/readiness probes.

Spring Kafka config (`application.yml`):
- `spring.kafka.bootstrap-servers: demo-kafka-kafka-bootstrap:9092`
- `spring.kafka.producer.key-serializer: StringSerializer`
- `spring.kafka.producer.value-serializer: JsonSerializer`
- `spring.kafka.producer.acks: all`
- `spring.kafka.producer.properties.enable.idempotence: true`

### `consumer` app

Same packaging conventions as the producer.

Behavior:
- `@KafkaListener(topics = "orders", groupId = "demo-consumer-group", concurrency = "3")` — three listener threads per pod so a single pod can own all 3 partitions when scaled to 1 replica.
- Validation: throws `InvalidOrderException` if `amount.signum() <= 0`.
- Success log line — includes pod name (from `HOSTNAME`) so it's obvious which pod processed which partition:
  ```
  processed orderId=ORD-1234 partition=2 offset=88 podName=consumer-abc-xyz
  ```
- Error handling: `DefaultErrorHandler` configured with:
  - `FixedBackOff(1000ms, 2)` — 2 retries with 1-second backoff.
  - On final failure, `DeadLetterPublishingRecoverer` writes the failed record to `orders-dlq`. Spring Kafka's default header propagation includes:
    - `kafka_dlt-exception-fqcn`
    - `kafka_dlt-exception-message`
    - `kafka_dlt-original-topic` / `partition` / `offset`
- Listener container ack mode: `AckMode.RECORD` — offsets committed per successful record so progression is visible in Kafka UI.

Spring Kafka config (`application.yml`):
- `spring.kafka.consumer.group-id: demo-consumer-group`
- `spring.kafka.consumer.auto-offset-reset: earliest`
- `spring.kafka.consumer.key-deserializer: StringDeserializer`
- `spring.kafka.consumer.value-deserializer: JsonDeserializer` (with trusted packages set to the `common` module's package)
- `spring.kafka.listener.ack-mode: RECORD`

### Kafka cluster (Strimzi `Kafka` CR)

- Cluster name: `demo-kafka` → bootstrap service `demo-kafka-kafka-bootstrap:9092`.
- KRaft mode (no ZooKeeper) — the 2026 Strimzi default.
- 1 broker node, ephemeral storage (`emptyDir`).
- Single plain listener on port 9092, internal only.
- Resource requests: `512Mi` memory / `200m` CPU per broker.

This is intentionally minimal for local-cluster footprint. The README will note that production deployments use 3 brokers, persistent storage, and TLS listeners.

### Topics (`KafkaTopic` CRs)

| Topic        | Partitions | Replicas | Retention | Notes |
|--------------|-----------:|---------:|-----------|-------|
| `orders`     | 3          | 1        | 1h        | 3 partitions is the sweet spot for the scaling demo |
| `orders-dlq` | 1          | 1        | 24h       | Longer retention so DLQ records stick around for inspection |

### Kafka UI

- Image: `provectuslabs/kafka-ui:latest` (pin a digest in the manifest for reproducibility).
- Configured via env var: `KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=demo-kafka-kafka-bootstrap:9092`.
- Service: ClusterIP port 8080.
- `Route` with edge TLS termination — OpenShift provides the certificate. This is the only externally exposed endpoint in the demo.

### Strimzi operator install

- An OLM `Subscription` to `strimzi-kafka-operator` from the `community-operators` catalog in `openshift-operators`.
- Applied via `oc apply -k k8s/` — `oc apply` is idempotent, so re-applying when the operator already exists is a no-op.
- README notes that AMQ Streams (Red Hat's supported Strimzi rebuild) users can comment out the `- strimzi-operator` line in the umbrella `kustomization.yaml`.

## Install / uninstall flow

**Prerequisites:** `oc login` to the cluster; cluster-admin (needed once, to install the Strimzi operator); Java 21 + Maven on the workstation.

**Install:**
```bash
mvn -q clean package
oc new-project kafka-demo
oc new-build --binary --name=producer --strategy=docker
oc new-build --binary --name=consumer --strategy=docker
oc start-build producer --from-dir=producer --follow
oc start-build consumer --from-dir=consumer --follow
oc apply -k k8s/
```

The `Deployment` manifests reference the `producer:latest` and `consumer:latest` ImageStreamTags produced by `new-build`.

**Verify:**
```bash
oc get kafka,kafkatopic,pods
oc get route kafka-ui -o jsonpath='{.spec.host}'
oc logs -f deploy/producer
oc logs -f deploy/consumer
```

**Learning exercises (documented in README):**
1. Open Kafka UI, browse the `orders` topic, see messages live.
2. `oc scale deploy/consumer --replicas=3` — observe rebalance in Kafka UI (Consumer Groups tab).
3. `oc scale deploy/consumer --replicas=5` — observe 2 pods sit idle (more consumers than partitions).
4. In Kafka UI, browse `orders-dlq` — see the bad records with their exception headers.
5. `oc delete pod -l app=consumer --field-selector=metadata.name=<one-pod>` — watch the group rebalance after a graceful leave.

**Uninstall:**
```bash
oc delete -k k8s/
# Optional, leaves Strimzi installed cluster-wide:
oc delete subscription strimzi-kafka-operator -n openshift-operators
```

## Testing approach

Deliberately minimal — the demo is the integration test.

- **`common`:** one unit test asserting `Order` JSON round-trips identically.
- **`producer`:** `@SpringBootTest` slice with `@EmbeddedKafka`; asserts the producer publishes with the configured key and that the bad-record cadence (every 7th) is correct.
- **`consumer`:** `@SpringBootTest` with `@EmbeddedKafka`; asserts a valid order is processed and an invalid order ends up on `orders-dlq` with the expected DLT headers.
- **No integration tests against a real OpenShift cluster** — `oc apply -k k8s/` *is* the integration test.

## Risks & open questions

- **Strimzi catalog availability.** If the cluster's OperatorHub doesn't have the community Strimzi operator (some restricted clusters disable community catalogs), the install fails at the `Subscription` step. The README will include a manual install fallback using Strimzi's upstream YAML.
- **Image build strategy.** `oc new-build --strategy=docker` requires Docker BuildConfig support on the cluster (standard on OpenShift, including CRC). If absent, the user can build locally with `docker build` and push to an external registry — documented as a fallback.
- **Local cluster resource pressure.** Kafka broker + Kafka UI + 1 producer + N consumers is ~1.5–2 GiB total. CRC's default 9 GiB allocation has headroom but users on smaller VMs may need to bump it.
- **Bad-record cadence is deterministic.** Every 7th record is bad. This is good for demonstration but means the DLQ fills predictably; if the user runs the demo for hours, retention on `orders-dlq` (24h) means a few thousand records. Acceptable.

## Future work (out of scope for v1)

- Add a third app: a Kafka Streams processor (`orders → enriched-orders`) to demonstrate stream processing.
- Add Schema Registry + Avro to demonstrate schema evolution.
- TLS listener + SCRAM auth + a `KafkaUser` CR — show production hardening.
- Prometheus + Grafana — JMX exporter sidecar on the broker.
