# KEDA + Kafka Autoscaling Demo (CDC-style consumer lag)

A minimal, reproducible demo showing [KEDA](https://keda.sh) autoscaling a Kubernetes
Deployment based on real Kafka consumer group lag — the same pattern used to scale
CDC (Change Data Capture) consumers reading from Debezium/Kafka pipelines in production.

This repo runs entirely on your laptop using [Kind](https://kind.sigs.k8s.io/)
(Kubernetes-in-Docker), [Strimzi](https://strimzi.io/) (Kafka on Kubernetes), and
KEDA. No cloud account required.

## What this demonstrates

- A Kafka topic with a consumer group falling behind (lag building up)
- A KEDA `ScaledObject` watching that lag via the built-in [Kafka scaler](https://keda.sh/docs/latest/scalers/apache-kafka/)
- The consumer Deployment automatically scaling from 1 → 3 replicas as lag crosses
  a threshold, and back down once lag clears

This mirrors a real production pattern: CDC pipelines (Debezium → Kafka → downstream
consumer) produce bursty, uneven load. A fixed replica count either wastes resources
at idle or can't keep up during a burst. KEDA lets the consumer scale to match load
automatically.

## Architecture

```
┌─────────────┐          ┌─---──────────┐        ┌──────────────────┐
│   Producer   │─────▶  |  Kafka Topic │◀─────  │   Consumer Group │
│ (CLI, ad-hoc)│         │ demo-topic   │        │  demo-consumer   │
└─────────────┘          │ (3 partitions)│       │  (1–5 replicas)  │
                         └──────────────┘        └──────────────────┘
                             ▲                        ▲
                             │ lag metric             │ scales
                             │                        │
                      ┌───────────────────────── ─────────┐
                      │      KEDA ScaledObject            │
                      │  (kafka scaler, lagThreshold=5)   │
                      └────────────────────── ────────────┘
```

- **Kafka**: single-broker cluster (KRaft mode, no Zookeeper) via the Strimzi operator
- **Consumer**: a throttled `kafka-console-consumer` (1 message every ~3s) to simulate
  a slow downstream consumer, e.g. one doing DB writes or enrichment per record
- **KEDA**: polls consumer group lag on `demo-topic` and adjusts replica count via
  a Kubernetes HPA it manages automatically

## Prerequisites

| Tool | Purpose | Install |
|---|---|---|
| Docker | Runs Kind's nodes and all pods | [docs.docker.com](https://docs.docker.com/get-docker/) |
| kubectl | Talk to the cluster | [kubernetes.io](https://kubernetes.io/docs/tasks/tools/) |
| Kind | Local Kubernetes cluster | [kind.sigs.k8s.io](https://kind.sigs.k8s.io/docs/user/quick-start/#installation) |
| Helm | Install KEDA and Strimzi | [helm.sh](https://helm.sh/docs/intro/install/) |

Tested on WSL2 (Ubuntu) with Docker Desktop's WSL integration enabled. Should work
identically on native Linux or macOS.

## Setup

### 1. Create the cluster

```bash
kind create cluster --name keda-demo
kubectl get nodes   # confirm STATUS: Ready
```

### 2. Install KEDA

```bash
helm repo add kedacore https://kedacore.github.io/charts
helm repo update
kubectl create namespace keda
helm install keda kedacore/keda --namespace keda
kubectl get pods -n keda   # wait for all pods 1/1 Running
```

### 3. Install the Strimzi Kafka operator

```bash
helm repo add strimzi https://strimzi.io/charts/
helm repo update
kubectl create namespace kafka
helm install strimzi-kafka-operator strimzi/strimzi-kafka-operator --namespace kafka
kubectl get pods -n kafka   # wait for the operator pod 1/1 Running
```

> **Note:** Bitnami's Kafka Helm chart is *not* used here — as of their August 2025
> licensing change, several image tags referenced by that chart (e.g.
> `bitnami/kafka:4.0.0-debian-12-r10`) have been pulled from the free Docker Hub
> registry, causing `ImagePullBackOff`. Strimzi is a CNCF project, actively
> maintained, and doesn't have this problem.

### 4. Deploy the Kafka cluster and topic

```bash
kubectl apply -f manifests/kafka-cluster.yaml
kubectl apply -f manifests/kafka-topic.yaml
kubectl get pods -n kafka -w   # wait for my-cluster-dual-role-0 and entity-operator to be Running
kubectl get kafkatopic -n kafka   # READY: True
```

### 5. Deploy the throttled consumer

```bash
kubectl apply -f manifests/kafka-consumer.yaml
kubectl get pods -n kafka -l app=demo-consumer
```

### 6. Deploy the KEDA ScaledObject

```bash
kubectl apply -f manifests/kafka-scaledobject.yaml
kubectl get scaledobject -n kafka   # READY: True
```

If `READY` stays `False`, check `kubectl logs -n keda -l app=keda-operator --tail=30`
— see [Troubleshooting](docs/TROUBLESHOOTING.md) for the most common cause.

## Running the demo

**Terminal 1 — watch the HPA that KEDA manages:**

```bash
kubectl get hpa -n kafka -w
```

**Terminal 2 — produce a burst of messages:**

```bash
kubectl run kafka-producer --rm -it --restart=Never \
  --image=quay.io/strimzi/kafka:1.1.0-kafka-4.3.0 -n kafka -- \
  bin/kafka-console-producer.sh \
  --bootstrap-server my-cluster-kafka-bootstrap:9092 \
  --topic demo-topic
```

Paste 30–40 short lines (one message per line), then `Ctrl+C` to exit.

**What you should see in Terminal 1:** within a few seconds, `TARGETS` climbs above
the `lagThreshold` (5) and `REPLICAS` increases (e.g. `15/5 (avg)` → 3 replicas).
As the extra replicas drain the backlog, lag drops back toward `0/5`. Kubernetes'
HPA then holds the higher replica count for its stabilization window (~5 min by
default) before scaling back down to 1 — this is standard HPA behavior, not a bug.

## Checking lag directly (bypassing KEDA)

Useful for confirming what KEDA itself is seeing:

```bash
kubectl run kafka-lag-check --rm -it --restart=Never \
  --image=quay.io/strimzi/kafka:1.1.0-kafka-4.3.0 -n kafka -- \
  bin/kafka-consumer-groups.sh \
  --bootstrap-server my-cluster-kafka-bootstrap:9092 \
  --describe --group demo-consumer-group
```

## Cleanup

```bash
kind delete cluster --name keda-demo
```

This removes everything — no separate teardown of Kafka/KEDA is needed since the
whole cluster goes with it.

## Why this differs from other KEDA Kafka samples

Most public KEDA + Kafka samples use a simple producer/consumer with steady load.
This demo intentionally throttles the consumer to simulate a **CDC-style consumer**
— one that does meaningful per-record work (e.g. writing to a downstream database,
as in a Debezium → Kafka → sink pipeline) and therefore can't keep up with bursty
upstream traffic without scaling out.

## Files

| File | Purpose |
|---|---|
| `manifests/kafka-cluster.yaml` | Strimzi `Kafka` + `KafkaNodePool` — single-broker, KRaft mode |
| `manifests/kafka-topic.yaml` | `KafkaTopic` — `demo-topic`, 3 partitions |
| `manifests/kafka-consumer.yaml` | Throttled consumer Deployment |
| `manifests/kafka-scaledobject.yaml` | KEDA `ScaledObject` with the Kafka scaler |
| `docs/TROUBLESHOOTING.md` | Real issues hit while building this, and their fixes |

## License

Apache-2.0, matching the [kedacore](https://github.com/kedacore) project convention.
