# KEDA + Kafka Autoscaling Demo

### Production-style Kafka consumer autoscaling using Kubernetes, KEDA, Go, and Kafka consumer-group lag

A reproducible Kubernetes demo showing how to automatically scale a Kafka consumer based on **real Kafka consumer-group lag** using **KEDA**.

The project started as a minimal KEDA + Kafka autoscaling experiment using a throttled Kafka CLI consumer and has evolved into a more production-style implementation with a custom **Go/Sarama Kafka consumer**, Kubernetes health probes, Prometheus instrumentation, container hardening, and graceful consumer lifecycle management.

The workload models a common pattern in event-driven and CDC systems:

```text
PostgreSQL / CDC Source
        │
        ▼
     Debezium
        │
        ▼
      Kafka
        │
        ▼
  Consumer Group
        │
        ▼
 Downstream Processing
```

When Kafka traffic increases faster than the consumer can process it, **consumer-group lag increases**. KEDA detects that lag and drives Kubernetes autoscaling so additional consumer replicas can process the backlog.

> **Environment:** Local Kubernetes using Kind
> **Kafka:** Strimzi
> **Autoscaling:** KEDA + Kubernetes HPA
> **Consumer:** Go + IBM Sarama
> **Status:** Core implementation and autoscaling flow validated locally

---

# What This Project Demonstrates

The project demonstrates the complete event-driven scaling loop:

```text
        Kafka message burst
                │
                ▼
        Consumer falls behind
                │
                ▼
        Consumer-group lag ↑
                │
                ▼
             KEDA
                │
                ▼
        Kubernetes HPA
                │
                ▼
      Consumer replicas ↑
                │
                ▼
       Kafka backlog drains
                │
                ▼
        Consumer lag → 0
```

The current implementation demonstrates:

* Kafka topic with multiple partitions
* Kafka consumer groups
* Custom Go Kafka consumer using Sarama
* Kubernetes Deployment
* KEDA Kafka scaler
* Kubernetes HPA integration
* Autoscaling based on consumer-group lag
* Liveness and readiness probes
* Prometheus metrics
* Graceful shutdown
* Configurable processing delay
* Multi-stage Docker build
* Distroless runtime image
* Non-root container execution
* CPU/memory requests and limits
* Unit tests for core application components
* Debugging Kafka consumer-group coordination
* Validation of scaling from 1 → 3 replicas
* Validation of backlog draining from non-zero lag → 0

---

# Architecture

```text
                         Local Kubernetes / Kind
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│                         Strimzi Kafka                               │
│                                                                     │
│                    ┌────────────────────┐                           │
│                    │     demo-topic     │                           │
│                    │    3 partitions    │                           │
│                    └─────────┬──────────┘                           │
│                              │                                      │
│                              │ Kafka records                         │
│                              ▼                                      │
│                 ┌──────────────────────────┐                        │
│                 │     demo-consumer         │                        │
│                 │                           │                        │
│                 │       Go + Sarama         │                        │
│                 │                           │                        │
│                 │   ┌────┐ ┌────┐ ┌────┐   │                        │
│                 │   │Pod1│ │Pod2│ │Pod3│   │                        │
│                 │   └────┘ └────┘ └────┘   │                        │
│                 │                           │                        │
│                 └────────────┬──────────────┘                        │
│                              │                                      │
│                              │ consumer-group lag                    │
│                              ▼                                      │
│                    ┌───────────────────┐                            │
│                    │       KEDA        │                            │
│                    │   Kafka scaler    │                            │
│                    └─────────┬─────────┘                            │
│                              │                                      │
│                              ▼                                      │
│                    ┌───────────────────┐                            │
│                    │        HPA        │                            │
│                    └─────────┬─────────┘                            │
│                              │                                      │
│                              ▼                                      │
│                       Deployment scale                              │
│                           1 → 5                                      │
│                                                                     │
│       ┌─────────────────────────────────────────────────────┐       │
│       │                    Application                      │       │
│       │                                                     │       │
│       │  /healthz       /readyz        /metrics             │       │
│       │      │              │              │                │       │
│       │      ▼              ▼              ▼                │       │
│       │  Liveness       Readiness      Prometheus            │       │
│       └─────────────────────────────────────────────────────┘       │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

# Why Kafka Lag?

A Kafka consumer can be perfectly healthy from a CPU perspective while still falling behind.

For example:

```text
CPU utilization: 35%
Memory utilization: 40%
Kafka lag:       10,000 messages
```

CPU-based autoscaling might decide that no additional replicas are required.

For an event-driven consumer, however, the backlog is the important signal.

This project therefore uses:

```text
Kafka Consumer Group Lag
```

as the autoscaling signal.

Conceptually:

```text
LOG-END-OFFSET - CURRENT-OFFSET = LAG
```

When lag increases above the configured KEDA threshold, KEDA activates the workload and Kubernetes increases the number of consumer replicas.

---

# Technology Stack

| Technology        | Role                                           |
| ----------------- | ---------------------------------------------- |
| Go                | Kafka consumer implementation                  |
| IBM Sarama        | Kafka client and consumer-group implementation |
| Apache Kafka      | Event streaming                                |
| Strimzi           | Kafka operator on Kubernetes                   |
| Kubernetes        | Container orchestration                        |
| Kind              | Local Kubernetes cluster                       |
| KEDA              | Event-driven autoscaling                       |
| Kubernetes HPA    | Replica management                             |
| Docker            | Containerization                               |
| Distroless        | Minimal production runtime                     |
| Prometheus Client | Application metrics                            |
| Helm              | Installation of KEDA and Strimzi               |
| kubectl           | Kubernetes administration                      |

---

# Why a Custom Go Consumer?

The original version of the project used a throttled:

```text
kafka-console-consumer.sh
```

to create consumer lag.

That was useful for proving the KEDA concept, but it did not represent a real application very well.

The project was therefore extended with a custom Go consumer.

The current application uses:

```text
Go
  +
IBM Sarama
  +
Kafka Consumer Groups
  +
HTTP Server
  +
Prometheus Metrics
```

This allows the project to demonstrate application-level engineering concerns such as:

* Consumer lifecycle
* Graceful shutdown
* Readiness
* Health checks
* Processing latency
* Success/failure metrics
* Resource configuration
* Container security

---

# Go Consumer Architecture

The application is structured into separate packages instead of putting the complete consumer inside `main.go`.

```text
app/
├── cmd/
│   └── consumer/
│       └── main.go
│
└── internal/
    ├── config/
    │   └── config.go
    │
    ├── consumer/
    │   ├── consumer.go
    │   └── consumer_test.go
    │
    ├── metrics/
    │   ├── metrics.go
    │   └── metrics_test.go
    │
    └── server/
        ├── server.go
        └── server_test.go
```

### `cmd/consumer`

Application entrypoint.

Responsible for:

* Loading configuration
* Creating the logger
* Initializing metrics
* Starting the HTTP server
* Connecting to Kafka
* Creating the consumer
* Handling shutdown

### `internal/config`

Handles application configuration from environment variables.

### `internal/consumer`

Contains the Kafka consumer-group implementation.

### `internal/metrics`

Contains Prometheus counters and histograms.

### `internal/server`

Contains the HTTP server and health/readiness/metrics endpoints.

---

# Kafka Consumer Groups

The application uses:

```text
demo-consumer-group
```

for its Kafka consumer group.

The topic contains three partitions:

```text
demo-topic

Partition 0
Partition 1
Partition 2
```

Kafka distributes partitions between members of the same consumer group.

For example:

```text
1 replica:

Consumer 1
 ├── Partition 0
 ├── Partition 1
 └── Partition 2


3 replicas:

Consumer 1 → Partition 0
Consumer 2 → Partition 1
Consumer 3 → Partition 2
```

This is important when designing autoscaling.

Adding more Kubernetes replicas does not create unlimited Kafka parallelism. The available parallelism is constrained by the number of partitions.

---

# KEDA Autoscaling

The KEDA `ScaledObject` monitors the Kafka consumer group.

The basic relationship is:

```text
Kafka
 │
 │ consumer-group lag
 ▼
KEDA Kafka Scaler
 │
 ▼
Kubernetes HPA
 │
 ▼
Deployment
```

The Deployment is configured with:

```text
Minimum replicas: 1
Maximum replicas: 5
```

The KEDA Kafka scaler uses the consumer-group lag threshold configured in:

```text
manifests/kafka-scaledobject.yaml
```

When the lag crosses the configured threshold, KEDA activates the scaler.

KEDA then works with the Kubernetes HPA to change the Deployment replica count.

---

# Important KEDA vs HPA Concept

KEDA and HPA have different responsibilities.

### KEDA

Answers:

> "Is there event-driven work that requires scaling?"

For this project:

```text
Kafka consumer lag
```

### HPA

Answers:

> "How many replicas should Kubernetes run?"

The resulting architecture is:

```text
Kafka lag
    │
    ▼
  KEDA
    │
    ▼
   HPA
    │
    ▼
Deployment replicas
```

This is why you may see both resources:

```bash
kubectl get scaledobject -n kafka
kubectl get hpa -n kafka
```

---

# Application Health

The Go application exposes three HTTP endpoints.

## `/healthz`

Used as the Kubernetes liveness probe.

It answers:

> Is the application process alive?

If the HTTP server is functioning, the endpoint returns success.

---

## `/readyz`

Used as the Kubernetes readiness probe.

It answers:

> Is the Kafka consumer ready to participate in consumption?

This is intentionally different from `/healthz`.

The HTTP server can start before the Kafka consumer has successfully established a consumer-group session.

Therefore:

```text
HTTP server started
        ≠
Kafka consumer ready
```

The application marks itself ready only after the Kafka consumer-group lifecycle reaches the appropriate ready state.

When consumption stops, readiness is also removed.

This prevents Kubernetes from treating a process as ready merely because its HTTP server is alive.

---

## `/metrics`

Exposes Prometheus-compatible application metrics.

The application tracks metrics around message processing, including:

```text
Messages processed
Messages failed
Message processing duration
```

This provides visibility into both throughput and processing behavior.

---

# Docker Design

The project uses a multi-stage Docker build.

```text
┌──────────────────────────┐
│     Go Builder Image     │
│                          │
│ go mod download          │
│ go build                 │
└────────────┬─────────────┘
             │
             │ static binary
             ▼
┌──────────────────────────┐
│ Distroless Runtime       │
│                          │
│ /consumer                │
│                          │
│ non-root                 │
└──────────────────────────┘
```

The final runtime image does not contain:

* Go compiler
* Go tooling
* Shell
* Package manager
* Development dependencies

The container runs as:

```text
nonroot:nonroot
```

This reduces the runtime attack surface compared with using the complete Go image in production.

---

# Kubernetes Deployment

The consumer Deployment contains:

```yaml
resources:
  requests:
    cpu: 100m
    memory: 64Mi
  limits:
    cpu: 500m
    memory: 256Mi
```

This provides explicit resource expectations to Kubernetes.

The container also defines:

```text
livenessProbe → /healthz
readinessProbe → /readyz
```

and exposes:

```text
HTTP_PORT=8080
```

---

# Configuration

The consumer is configured using environment variables.

| Variable               | Example                                                   | Purpose                    |
| ---------------------- | --------------------------------------------------------- | -------------------------- |
| `KAFKA_BROKERS`        | `my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092` | Kafka broker               |
| `KAFKA_TOPIC`          | `demo-topic`                                              | Topic to consume           |
| `KAFKA_CONSUMER_GROUP` | `demo-consumer-group`                                     | Consumer group             |
| `PROCESSING_DELAY_MS`  | `3000`                                                    | Simulated processing delay |
| `HTTP_PORT`            | `8080`                                                    | HTTP server port           |

`PROCESSING_DELAY_MS` is intentionally configurable because it makes it easy to reproduce backlog and autoscaling behavior.

---

# Prerequisites

The project runs locally and does not require a cloud account.

| Tool    | Purpose                            |
| ------- | ---------------------------------- |
| Docker  | Runs Kind and Kubernetes workloads |
| kubectl | Kubernetes CLI                     |
| Kind    | Local Kubernetes cluster           |
| Helm    | Install KEDA and Strimzi           |

The original demo was developed on WSL2/Ubuntu with Docker integration and is intended to work on native Linux and macOS as well.

---

# Installation

## 1. Create the Kind Cluster

```bash
kind create cluster --name keda-demo
```

Verify:

```bash
kubectl get nodes
```

The node should show:

```text
STATUS: Ready
```

---

# 2. Install KEDA

```bash
helm repo add kedacore https://kedacore.github.io/charts
helm repo update

kubectl create namespace keda

helm install keda \
  kedacore/keda \
  --namespace keda
```

Verify:

```bash
kubectl get pods -n keda
```

---

# 3. Install Strimzi

```bash
helm repo add strimzi https://strimzi.io/charts/
helm repo update

kubectl create namespace kafka

helm install strimzi-kafka-operator \
  strimzi/strimzi-kafka-operator \
  --namespace kafka
```

Verify:

```bash
kubectl get pods -n kafka
```

Wait until the Strimzi operator is running.

---

# 4. Deploy Kafka

```bash
kubectl apply -f manifests/kafka-cluster.yaml
kubectl apply -f manifests/kafka-topic.yaml
```

Check:

```bash
kubectl get pods -n kafka
kubectl get kafkatopic -n kafka
```

The Kafka cluster used by this demo is a local single-broker setup managed by Strimzi.

---

# 5. Build the Consumer Image

From the repository root:

```bash
docker build -t keda-kafka-consumer:dev .
```

Verify:

```bash
docker images keda-kafka-consumer:dev
```

---

# 6. Load the Image into Kind

Because this is a local Kind environment, the image does not need to be pushed to Docker Hub or another registry.

```bash
kind load docker-image \
  keda-kafka-consumer:dev \
  --name keda-demo
```

Verify that the image exists inside the Kind node:

```bash
docker exec keda-demo-control-plane \
  crictl images | grep keda-kafka-consumer
```

---

# 7. Deploy the Go Consumer

```bash
kubectl apply -f manifests/kafka-consumer.yaml
```

Check rollout:

```bash
kubectl rollout status \
  deployment/demo-consumer \
  -n kafka
```

Check the pod:

```bash
kubectl get pods \
  -n kafka \
  -l app=demo-consumer
```

---

# 8. Deploy KEDA ScaledObject

```bash
kubectl apply \
  -f manifests/kafka-scaledobject.yaml
```

Check:

```bash
kubectl get scaledobject -n kafka
```

Expected:

```text
READY: True
```

You can also inspect the HPA generated by KEDA:

```bash
kubectl get hpa -n kafka
```

---

# Running the Autoscaling Demo

Use multiple terminals.

## Terminal 1 — Watch KEDA

```bash
kubectl get scaledobject -n kafka -w
```

You can observe:

```text
ACTIVE: False
```

when there is no backlog.

When Kafka lag increases:

```text
ACTIVE: True
```

---

## Terminal 2 — Watch the HPA

```bash
kubectl get hpa -n kafka -w
```

Example:

```text
NAME                                  TARGETS
keda-hpa-demo-consumer-scaledobject   0/5
```

After backlog is created:

```text
NAME                                  TARGETS
keda-hpa-demo-consumer-scaledobject   15/5
```

and the replica count can increase:

```text
REPLICAS
3
```

---

## Terminal 3 — Watch Consumer Pods

```bash
kubectl get pods \
  -n kafka \
  -l app=demo-consumer \
  -w
```

You can observe:

```text
1 pod
 ↓
2 pods
 ↓
3 pods
```

as KEDA/HPA reacts to the Kafka backlog.

---

# Generate Kafka Load

You can produce messages using the Strimzi Kafka CLI image:

```bash
kubectl run kafka-producer \
  --rm -it \
  --restart=Never \
  --image=quay.io/strimzi/kafka:1.1.0-kafka-4.3.0 \
  -n kafka -- \
  bin/kafka-console-producer.sh \
  --bootstrap-server my-cluster-kafka-bootstrap:9092 \
  --topic demo-topic
```

Enter multiple messages:

```text
message-1
message-2
message-3
...
message-100
```

Then press:

```text
Ctrl+C
```

---

# Inspect Kafka Consumer Lag

KEDA is ultimately reacting to Kafka consumer-group lag, so it is useful to inspect the source directly.

```bash
kubectl run kafka-group-check \
  --rm -it \
  --restart=Never \
  --image=quay.io/strimzi/kafka:1.1.0-kafka-4.3.0 \
  -n kafka -- \
  bin/kafka-consumer-groups.sh \
  --bootstrap-server my-cluster-kafka-bootstrap:9092 \
  --describe \
  --group demo-consumer-group
```

Example:

```text
GROUP               TOPIC        PARTITION
demo-consumer-group demo-topic   0
demo-consumer-group demo-topic   1
demo-consumer-group demo-topic   2
```

The important columns are:

```text
CURRENT-OFFSET
LOG-END-OFFSET
LAG
```

For example:

```text
CURRENT-OFFSET    LOG-END-OFFSET    LAG
3                 100               97
```

means that the consumer is 97 messages behind the latest available offset.

After scaling and processing:

```text
CURRENT-OFFSET    LOG-END-OFFSET    LAG
100               100               0
```

---

# Validated Autoscaling Result

One of the main validation runs produced the following behavior.

### Initial state

```text
Deployment replicas: 1
Kafka lag: 0
KEDA ACTIVE: False
```

### Message burst

A burst of 100 records was introduced.

The consumer intentionally processes slowly using:

```text
PROCESSING_DELAY_MS=3000
```

Kafka lag increased.

Example:

```text
CURRENT-OFFSET    LOG-END-OFFSET    LAG
3                 100               97
```

### KEDA activation

KEDA detected the backlog:

```text
ACTIVE: True
```

The HPA target increased and Kubernetes started additional consumer pods.

### Scale-out

The Deployment reached:

```text
3 replicas
```

with all replicas successfully joining:

```text
demo-consumer-group
```

### Backlog drain

The additional consumers processed the Kafka partitions.

Eventually:

```text
CURRENT-OFFSET    LOG-END-OFFSET    LAG
100               100               0
```

The backlog was completely consumed.

This validated the complete event-driven autoscaling path.

---

# Important Kafka Scaling Limitation

This project intentionally uses:

```text
3 Kafka partitions
```

and allows the Deployment to scale up to:

```text
5 replicas
```

However, Kafka consumer-group parallelism is constrained by partition count.

With three partitions:

```text
3 partitions
+
3 active consumers
=
maximum useful parallelism for this topic
```

A fourth or fifth consumer may exist as a Kafka group member, but there is no additional partition available for it to consume.

This is an important production design consideration:

> **Kafka partition count must be considered when choosing the maximum number of consumer replicas.**

---

# Debugging Journey

This project was not built as a static YAML exercise. Several issues were encountered while moving from the original demo to the custom consumer.

## Kafka connectivity from containers

One common issue was using:

```text
localhost:9092
```

from inside a container.

Inside Kubernetes, `localhost` refers to the current container/pod, not the Kafka broker.

The application therefore uses Kubernetes service discovery:

```text
my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092
```

This allows the consumer to reach the Strimzi Kafka service.

---

## Consumer group protocol issue

During development, the Go consumer encountered a consumer-group coordination problem:

```text
The provider group protocol type is incompatible with the other members
```

The important lesson was that Kafka consumer groups are not simply a list of unrelated consumers.

All members of a group must participate in compatible group coordination.

The group was inspected using:

```bash
kafka-consumer-groups.sh \
  --describe \
  --group demo-consumer-group
```

After removing the conflicting consumer and allowing the Sarama consumers to coordinate normally, the consumer group stabilized.

---

## Readiness race

Another important design issue was distinguishing:

```text
HTTP server started
```

from:

```text
Kafka consumer ready
```

The application initially could start its HTTP server before Kafka consumer-group assignment completed.

Therefore, readiness was moved to the Kafka consumer lifecycle.

The resulting lifecycle is:

```text
Application starts
      │
      ▼
HTTP server starts
      │
      ▼
Kafka connection established
      │
      ▼
Consumer group session starts
      │
      ▼
Readiness = true
```

If consumption stops:

```text
Consumer session ends
      │
      ▼
Readiness = false
```

This is much closer to how a production service should behave.

---

# Container Security

The Docker image uses:

```text
Multi-stage build
+
Static Go binary
+
Distroless runtime
+
Non-root user
```

The runtime image does not contain the Go development environment.

The container entrypoint is:

```text
/consumer
```

and it runs as:

```text
nonroot:nonroot
```

This keeps the runtime image small and minimizes unnecessary packages and privileges.

---

# Resource Management

The Kubernetes Deployment defines explicit resource requests:

```yaml
requests:
  cpu: 100m
  memory: 64Mi
```

and limits:

```yaml
limits:
  cpu: 500m
  memory: 256Mi
```

This gives Kubernetes enough information to schedule the workload while preventing an individual consumer pod from consuming unlimited resources.

---

# Observability

The application exposes Prometheus-compatible metrics.

The current application instrumentation includes metrics for:

* Successfully processed messages
* Failed messages
* Message-processing duration

The endpoint is:

```text
/metrics
```

Example port-forwarding can be used for local inspection:

```bash
kubectl port-forward \
  deployment/demo-consumer \
  8080:8080 \
  -n kafka
```

Then:

```text
http://localhost:8080/metrics
```

---

# Testing

The Go application includes unit tests for the core components.

Run:

```bash
cd app
go test ./...
```

Expected result:

```text
?    github.com/Upshivam786/keda-kafka-demo/app/cmd/consumer
ok   github.com/Upshivam786/keda-kafka-demo/app/internal/config
ok   github.com/Upshivam786/keda-kafka-demo/app/internal/consumer
ok   github.com/Upshivam786/keda-kafka-demo/app/internal/metrics
ok   github.com/Upshivam786/keda-kafka-demo/app/internal/server
```

The tests cover the application-level behavior of:

```text
Consumer
Metrics
HTTP server
Health endpoints
Readiness behavior
```

---

# Build Verification

The application can be compiled independently:

```bash
cd app
go build -o consumer ./cmd/consumer
```

The repository also supports building the production-style container:

```bash
cd ..
docker build -t keda-kafka-consumer:dev .
```

---

# Repository Structure

```text
keda-kafka-demo/
│
├── app/
│   ├── cmd/
│   │   └── consumer/
│   │       └── main.go
│   │
│   ├── internal/
│   │   ├── config/
│   │   ├── consumer/
│   │   │   ├── consumer.go
│   │   │   └── consumer_test.go
│   │   ├── metrics/
│   │   │   ├── metrics.go
│   │   │   └── metrics_test.go
│   │   └── server/
│   │       ├── server.go
│   │       └── server_test.go
│   │
│   ├── go.mod
│   └── go.sum
│
├── manifests/
│   ├── kafka-cluster.yaml
│   ├── kafka-topic.yaml
│   ├── kafka-consumer.yaml
│   └── kafka-scaledobject.yaml
│
├── docs/
│   └── TROUBLESHOOTING.md
│
├── Dockerfile
├── .dockerignore
├── .gitignore
└── README.md
```

---

# Files

| File                                | Purpose                                              |
| ----------------------------------- | ---------------------------------------------------- |
| `Dockerfile`                        | Multi-stage Go → distroless container build          |
| `app/cmd/consumer/main.go`          | Application entrypoint                               |
| `app/internal/config/`              | Environment-based configuration                      |
| `app/internal/consumer/`            | Kafka consumer-group implementation                  |
| `app/internal/metrics/`             | Prometheus instrumentation                           |
| `app/internal/server/`              | HTTP server, health, readiness and metrics endpoints |
| `manifests/kafka-cluster.yaml`      | Strimzi Kafka cluster                                |
| `manifests/kafka-topic.yaml`        | `demo-topic` configuration                           |
| `manifests/kafka-consumer.yaml`     | Go consumer Deployment                               |
| `manifests/kafka-scaledobject.yaml` | KEDA Kafka scaler                                    |
| `docs/TROUBLESHOOTING.md`           | Development and troubleshooting notes                |

---

# Original Demo → Current Implementation

The project has evolved in several stages.

## Original implementation

```text
Kafka
  ↓
throttled kafka-console-consumer
  ↓
consumer-group lag
  ↓
KEDA
  ↓
HPA
```

This established that KEDA could detect Kafka lag and scale the Kubernetes Deployment.

## Current implementation

```text
Kafka
  ↓
Go + Sarama consumer
  ↓
consumer group
  ↓
health/readiness lifecycle
  ↓
Prometheus metrics
  ↓
KEDA Kafka scaler
  ↓
HPA
  ↓
multiple Go consumer replicas
```

The current version therefore focuses not only on demonstrating KEDA but also on the engineering required to turn a Kafka consumer into a Kubernetes workload.

---

# Production Relevance

The pattern demonstrated here is directly applicable to event-driven systems such as:

```text
PostgreSQL
    │
    ▼
 Debezium
    │
    ▼
  Kafka
    │
    ├───────────────┐
    ▼               ▼
Consumer A       Consumer B
    │               │
    ▼               ▼
Database         Search / Analytics
```

For a CDC pipeline, downstream processing speed can vary depending on:

* Database write latency
* Transformation complexity
* External API calls
* Enrichment operations
* Traffic bursts
* Batch sizes

A fixed number of consumers can therefore become inefficient.

KEDA allows the Kubernetes workload to respond to the event-stream backlog rather than relying only on infrastructure utilization.

---

# What I Learned

This project provided hands-on experience with:

### Kafka

* Topics
* Partitions
* Consumer groups
* Offsets
* Consumer lag
* Consumer-group coordination
* Partition-based parallelism

### Kubernetes

* Deployments
* Replica management
* Service discovery
* Resource requests/limits
* Liveness probes
* Readiness probes
* Rolling deployments
* Kind-based local clusters

### KEDA

* `ScaledObject`
* Kafka scaler
* Event-driven scaling
* KEDA → HPA integration
* Kafka lag as a scaling signal

### Application Engineering

* Go
* Sarama
* Graceful shutdown
* Configuration management
* Structured logging
* Prometheus metrics
* Unit testing

### Container Engineering

* Multi-stage Docker builds
* Static Go binaries
* Distroless images
* Non-root containers
* Local image loading into Kind

### Troubleshooting

* Kubernetes DNS
* Container networking
* Kafka consumer-group coordination
* Readiness race conditions
* Kafka lag verification
* HPA/KEDA behavior

---

# Future Roadmap

The current implementation establishes the core architecture. Planned improvements include:

## Observability

* [ ] Deploy Prometheus
* [ ] Deploy Grafana
* [ ] Kafka lag dashboard
* [ ] Consumer throughput dashboard
* [ ] Processing latency dashboard
* [ ] Alerting for sustained lag
* [ ] Alerting for consumer failures

## Reliability

* [ ] Retry strategy
* [ ] Dead-letter topic
* [ ] Exponential backoff
* [ ] Idempotent message processing
* [ ] Explicit offset/error handling strategy
* [ ] PodDisruptionBudget

## Security

* [ ] Kafka TLS
* [ ] SASL authentication
* [ ] Kubernetes Secrets
* [ ] NetworkPolicy
* [ ] Additional container security context
* [ ] Image vulnerability scanning

## CI/CD

* [ ] GitHub Actions
* [ ] Automated Go tests
* [ ] `go vet` / static analysis
* [ ] Docker image build
* [ ] Container vulnerability scanning
* [ ] Image publishing
* [ ] Automated Kubernetes deployment

## Production Kubernetes

* [ ] Helm chart
* [ ] Environment-specific values
* [ ] Pod anti-affinity
* [ ] PodDisruptionBudget
* [ ] Horizontal scaling tuning
* [ ] Graceful rolling deployment strategy
* [ ] Managed Kubernetes deployment

## Kafka

* [ ] Multi-broker cluster
* [ ] Replication factor > 1
* [ ] Production storage configuration
* [ ] TLS/SASL
* [ ] Kafka topic retention strategy
* [ ] Partition scaling strategy

---

# Cleanup

Because the entire demonstration runs inside a Kind cluster, cleanup is simple:

```bash
kind delete cluster --name keda-demo
```

This removes:

* Kubernetes resources
* Kafka
* Strimzi
* KEDA
* Consumer pods
* HPA
* ScaledObjects
* Topics

---

# Why This Project?

The goal is not simply to deploy Kafka on Kubernetes.

The goal is to demonstrate an important production engineering pattern:

> **Use workload-specific signals to scale event-driven systems.**

For this workload:

```text
CPU utilization
       ↓
   not enough
       ↓
Kafka consumer lag
       ↓
better scaling signal
       ↓
KEDA
       ↓
HPA
       ↓
consumer replicas
```

The project combines Kafka, Kubernetes, KEDA, Go, Docker, health management, observability, and autoscaling into one reproducible system.

---

# License

Apache-2.0.

---

# Author

**Shivam Upadhyay**

DevOps / MLOps Engineer

Areas of interest:

* Kubernetes & Cloud Infrastructure
* DevOps & CI/CD
* Kafka & Event-driven Systems
* CDC Pipelines
* MLOps
* AI Infrastructure
* Observability
* RAG & LLM Systems

---

**Built and maintained by Shivam Upadhyay**
