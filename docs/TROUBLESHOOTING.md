# Troubleshooting

Real issues encountered while building this demo, kept here so others hit fewer
dead ends.

## `ImagePullBackOff` on Bitnami's Kafka chart

**Symptom:**
```
Failed to pull image "docker.io/bitnami/kafka:4.0.0-debian-12-r10":
... not found
```

**Cause:** Since Bitnami's August 2025 change to a paid "Secure Images" model, many
previously free image tags (including several referenced by their public Helm
charts) are no longer available on Docker Hub's free tier.

**Fix:** Use the [Strimzi](https://strimzi.io/) Kafka operator instead — a CNCF
project actively maintained specifically for running Kafka on Kubernetes. This repo
uses Strimzi throughout.

## `no matches for kind "Kafka" in version "kafka.strimzi.io/v1beta2"`

**Symptom:**
```
resource mapping not found for name: "my-cluster" ... no matches for kind "Kafka"
in version "kafka.strimzi.io/v1beta2"
ensure CRDs are installed first
```

**Cause:** This looks like a missing-CRD error, but the CRDs were actually
installed correctly (`kubectl get crd | grep strimzi` confirmed it). The real
issue was an API version mismatch — Strimzi operator 1.1.0 serves the `Kafka` and
`KafkaNodePool` CRDs at `v1`, not `v1beta2`.

**Fix:** Check what version your installed CRD actually serves before assuming
CRDs are missing:
```bash
kubectl get crd kafkas.kafka.strimzi.io -o jsonpath='{.spec.versions[*].name}'
```
Then match your manifest's `apiVersion` to that output.

## KEDA `ScaledObject` stuck at `READY: False`

**Symptom:** `kubectl get scaledobject -n kafka` shows `READY: False`, and
`kubectl logs -n keda -l app=keda-operator --tail=30` shows:
```
error creating kafka client: kafka: client has run out of available brokers
to talk to: dial tcp: lookup my-cluster-kafka-bootstrap on 10.96.0.10:53: no such host
```

**Cause:** Kubernetes' short-form DNS names (e.g. `my-cluster-kafka-bootstrap`)
only resolve automatically **within the same namespace**. The KEDA operator runs
in the `keda` namespace; Kafka runs in the `kafka` namespace. A pod *inside* the
`kafka` namespace (like the demo consumer) can use the short name just fine — only
cross-namespace lookups need the fully-qualified form.

**Fix:** Use the fully-qualified DNS name in the `ScaledObject`'s
`bootstrapServers` field:
```yaml
bootstrapServers: my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092
```

## Consumer processes all messages instantly instead of building lag

**Symptom:** After producing 15-20 messages, `kafka-consumer-groups.sh --describe`
shows `LAG: 0` for every partition — the demo consumer drained everything before
you could observe scaling.

**Cause:** `--consumer-property max.poll.records=1` on `kafka-console-consumer.sh`
limits how many records are fetched per poll, but doesn't meaningfully throttle a
console consumer that has nothing else to do — it just polls again immediately.

**Fix:** Force an explicit delay between messages by wrapping the consumer in a
loop that processes exactly one message then sleeps:
```bash
while true; do
  bin/kafka-console-consumer.sh \
    --bootstrap-server my-cluster-kafka-bootstrap:9092 \
    --topic demo-topic \
    --group demo-consumer-group \
    --max-messages 1
  sleep 3
done
```
This is what `manifests/kafka-consumer.yaml` does.

## `REPLICAS` stays high after lag drops to 0

**Symptom:** `kubectl get hpa -n kafka` shows `TARGETS: 0/5` but `REPLICAS` is
still 3, and it doesn't seem to be going down.

**Cause:** This is expected Kubernetes HPA behavior, not a bug. The HPA has a
scale-down stabilization window (5 minutes by default) — it remembers the
*highest* recent recommendation over that window and won't reduce replicas until
the whole window has passed without a higher spike, to avoid rapid flapping.

**Confirm this is what's happening:**
```bash
kubectl describe hpa keda-hpa-<scaledobject-name> -n kafka
```
Look for:
```
AbleToScale  True  ScaleDownStabilized  recent recommendations were higher
than current one, applying the highest recent recommendation
```
If you see that reason, just wait — it will scale down on its own once the
window clears.

## `CooldownPeriod is configured but is not relevant` warning

**Symptom:**
```
Warning: CooldownPeriod is configured but is not relevant. CooldownPeriod is
only relevant when minReplicaCount = 0 or idleReplicaCount = 0
```

**Cause:** `cooldownPeriod` only affects the transition to/from zero replicas.
This demo uses `minReplicaCount: 1`, so it's a harmless no-op warning.

**Fix:** None needed — safe to ignore, or remove `cooldownPeriod` from the
manifest if you want to silence it.
