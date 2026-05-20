# kafka-demo

## What this is

A hands-on Kafka demo for a local OpenShift cluster (CRC). Install it once, then watch messages flow through a producer and consumer, scale the consumer group up and down, observe DLQ behavior on bad records, and use Kafka UI to visualize topics, partitions, lag, and consumer-group rebalances in real time.

A Spring Boot **producer** emits `Order` JSON messages to a 3-partition `orders` topic; a Spring Boot **consumer** (`@KafkaListener`, group `demo-consumer-group`) validates them and routes bad records to `orders-dlq` after 2 retries. **Strimzi** operates a single-broker Kafka cluster (KRaft, ephemeral storage). **Kafka UI** (Provectus) exposes a web console via an OpenShift Route.

See the design doc for background and rationale: [docs/superpowers/specs/2026-05-20-kafka-openshift-demo-design.md](docs/superpowers/specs/2026-05-20-kafka-openshift-demo-design.md).

## Prerequisites

- A running local OpenShift cluster (CRC) with cluster-admin access. Cluster-admin is needed once, to install the Strimzi operator into `openshift-operators`.
- `oc` CLI logged in to the cluster. `oc whoami` should return your user (typically `kubeadmin`).
- For running the local unit tests only: JDK 21 and Apache Maven 3.9.x. These are **not** needed for the in-cluster install — the OpenShift binary build compiles everything from source.

## Repository layout

```
kafka-demo/
├── common/                       # Shared Order model + JSON (de)serializers
├── producer/                     # Spring Boot producer (KafkaTemplate)
├── consumer/                     # Spring Boot consumer (@KafkaListener + DLQ)
├── k8s/
│   ├── namespace/                # kafka-demo Namespace
│   ├── strimzi-operator/         # OLM Subscription for Strimzi (cluster-scoped)
│   ├── kafka-cluster/            # Kafka CR (KRaft, 1 broker, ephemeral)
│   ├── topics/                   # KafkaTopic CRs: orders, orders-dlq
│   ├── apps/                     # ImageStreams, BuildConfigs, Deployments, Services
│   ├── kafka-ui/                 # Provectus Kafka UI Deployment, Service, Route
│   └── kustomization.yaml        # Umbrella (advanced — see Install)
├── docs/                         # Design doc and notes
└── pom.xml                       # Multi-module Maven build
```

## Install

The user's namespace is `kafka-demo`. Apply in this order — Strimzi CRDs need to be installed by OLM before the Kafka CRs reference them:

```bash
# 1. Create the namespace
oc apply -k k8s/namespace/

# 2. Install Strimzi via OLM Subscription (cluster-scoped, into openshift-operators)
oc apply -k k8s/strimzi-operator/

# Wait for the Strimzi CRDs to register — usually ~30s
oc wait --for=condition=Established --timeout=300s \
  crd/kafkas.kafka.strimzi.io \
  crd/kafkanodepools.kafka.strimzi.io \
  crd/kafkatopics.kafka.strimzi.io

# 3. Create the Kafka cluster (KRaft, single broker, ephemeral)
oc apply -k k8s/kafka-cluster/

# Wait for the cluster (5-10 minutes on first install — image pulls)
oc wait kafka/demo-kafka -n kafka-demo --for=condition=Ready --timeout=600s

# 4. Create the topics (orders, orders-dlq)
oc apply -k k8s/topics/

# 5. Apply the producer + consumer manifests (ImageStreams, BuildConfigs, Deployments, Services)
oc apply -k k8s/apps/

# 6. Build the images from source (binary build, repo root as context)
oc start-build producer -n kafka-demo --from-dir=. --follow
oc start-build consumer -n kafka-demo --from-dir=. --follow

# The Deployments will roll automatically once their ImageStreams update to :latest.

# 7. Kafka UI
oc apply -k k8s/kafka-ui/

# Get the Kafka UI URL (open it in a browser)
oc get route kafka-ui -n kafka-demo -o jsonpath='https://{.spec.host}{"\n"}'
```

There is an umbrella kustomization at `k8s/kustomization.yaml`. Running `oc apply -k k8s/` in one shot **may** work after Strimzi has been installed once before on the same cluster, but on a first install the CRDs aren't registered when the Kafka CRs are applied, and the apply will fail. The ordered approach above is reliable; the umbrella exists for users who know what they're doing.

## Verify

```bash
oc get kafka,kafkatopic,deploy,pods -n kafka-demo

# Watch producer logs — should see "sent orderId=ORD-N..." every second
oc logs -f deploy/producer -n kafka-demo

# Watch consumer logs — should see "processed orderId=ORD-N partition=P offset=O podName=consumer-..."
oc logs -f deploy/consumer -n kafka-demo
```

In Kafka UI (open the Route URL from step 7), navigate to **Topics → orders** to see records flowing live.

## Learning exercises

### A. See partitioning by message key

Watch the producer log. `customerId=cust-N` consistently maps to a specific partition, because the customerId is used as the Kafka message key — same key, same partition.

### B. Scale the consumer to 3 replicas (one per partition)

```bash
oc scale deploy/consumer -n kafka-demo --replicas=3
```

In Kafka UI → **Consumers → demo-consumer-group**, watch the partition assignments rebalance across the three pods. Each pod's log will start showing only some partition numbers.

### C. Scale beyond the partition count

```bash
oc scale deploy/consumer -n kafka-demo --replicas=5
```

Two pods sit idle — more consumers than partitions means the extras get no assignment. Confirm in Kafka UI.

### D. Inspect the DLQ

Every 7th order has a negative amount and gets retried twice, then routed to `orders-dlq`. In Kafka UI → **Topics → orders-dlq**, browse messages — each carries headers like `kafka_dlt-exception-fqcn`, `kafka_dlt-exception-cause-fqcn`, `kafka_dlt-original-topic`, `kafka_dlt-original-partition`, `kafka_dlt-original-offset`.

### E. Trigger a rebalance via graceful pod removal

```bash
oc delete pod -l app=consumer -n kafka-demo --field-selector=metadata.name=<one-pod-name>
```

Watch Kafka UI's **Consumers** view re-assign partitions as the group rebalances.

## Uninstall

```bash
# Remove all in-namespace resources
oc delete -k k8s/

# Remove the namespace itself (cleanup any lingering resources)
oc delete namespace kafka-demo --ignore-not-found

# OPTIONAL — leaves Strimzi installed for other workloads:
oc delete subscription strimzi-kafka-operator -n openshift-operators --ignore-not-found
oc get csv -n openshift-operators -o name | grep -i strimzi | xargs -r oc delete -n openshift-operators
```

## Run the local unit tests

```bash
mvn -q test
```

Runs the 4 common tests + 2 producer + 2 consumer tests. `@EmbeddedKafka` spins up an in-process Kafka, so no cluster is needed.

## Deliberate simplifications

- **Single broker, ephemeral storage.** Fine for learning; data is lost on broker restart. Production would use 3 brokers, persistent storage, and anti-affinity.
- **Plain listener, no TLS or SCRAM.** Production would add a TLS listener and a `KafkaUser` CR.
- **No Schema Registry / Avro.** JSON on the wire keeps the demo readable. Production would use Avro or Protobuf with a Schema Registry.
- **No Prometheus / Grafana.** Kafka UI plus `oc logs` is enough for learning.

## Troubleshooting

- **`oc apply -k k8s/kafka-cluster/` fails with "no matches for kind Kafka".** Strimzi CRDs haven't registered yet. Run the `oc wait --for=condition=Established crd/...` from step 2 first, or just retry the apply 30 seconds later.
- **`Kafka demo-kafka` stuck NotReady for more than 10 min.** Check `oc describe kafka demo-kafka -n kafka-demo` and `oc get events -n kafka-demo --sort-by=.lastTimestamp`. CRC's default 9 GiB RAM allocation is tight; if the broker pod is OOMKilled, bump `crc config set memory 12288`, restart CRC, and re-apply.
- **Builds fail at `oc start-build`.** Confirm the BuildConfig exists: `oc get bc -n kafka-demo`. The `--from-dir=.` must be run from the repo root. If Buildah complains about pulling base images, your cluster may need `oc patch image.config.openshift.io/cluster --type=merge -p '{"spec":{"registrySources":{"insecureRegistries":[]}}}'` or a mirrored registry.
- **CRC has stale Strimzi from a previous demo.** Check `oc get csv -n openshift-operators | grep -i strimzi`. If a non-`stable`-channel CSV exists, delete it and the Subscription before reapplying.
