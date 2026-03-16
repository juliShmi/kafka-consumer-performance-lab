# Kafka Consumer Performance Lab

A small Spring Boot pet project to **load Kafka with “orders”** and observe **consumer lag** and latency. The goal is to learn Kafka consumer configuration and see how changes (especially **listener concurrency**) affect lag under a controlled workload.

---

### What it demonstrates

- **Load generator**: an HTTP endpoint produces many messages into Kafka topic `order-create`.
- **Consumer under test**: consumes messages with **manual acknowledgments** and a configurable artificial delay (simulated “work”).
- **Poison messages**: `POISON:` payloads go to `order-create.DLT` (to validate retry/DLT behavior).
- **Monitoring**: Prometheus + Grafana + kafka-exporter.

---

### Result: concurrency reduced lag

The consumer simulates work by sleeping for ~`100ms` per message:

- **1 thread** → ~10 msg/sec
- **3 threads** (`spring.kafka.listener.concurrency=3`, with \(\ge\) 3 partitions) → ~30 msg/sec

Under the same producer rate, this typically means **~2–3× lower steady-state consumer lag**.

---

### Starting point (baseline)

Originally it was a **single-thread consumer** (default concurrency \(=1\)) with manual ack and a fixed `100ms` delay to make lag visible. Later, adding **`concurrency=3`** reduced lag under the same load.

---

### Tech stack

- **Java 21**, **Spring Boot**, **Spring Kafka**
- **Micrometer + Actuator Prometheus**
- **Docker Compose** stack:
  - Kafka + Zookeeper (Confluent images)
  - Kafka UI
  - kafka-exporter
  - Prometheus
  - Grafana

---

### Quick start

Start infrastructure (Kafka, monitoring, UIs):

```bash
docker compose up -d
```

Run the Spring Boot app on the host (default port `8080`):

```bash
./mvnw spring-boot:run
```

Generate load:

```bash
curl -X POST "http://localhost:8080/orders/generate?count=10000"
```

Generate a mixed dataset (good + poison → DLT):

```bash
curl -X POST "http://localhost:8080/orders/generate-test?goodCount=1000&poisonCount=10"
```

---

### Where to look

- **Grafana**: `http://localhost:3000`
  - login: `admin` / `adminlocal`
  - dashboard: **“Kafka Overview (kafka-exporter)”**

- **Prometheus**: `http://localhost:9090`
- **Kafka UI**: `http://localhost:8085`
- **App metrics**: `http://localhost:8080/actuator/prometheus`

---

### Kafka names used

- **Topic**: `order-create`
- **Consumer group**: `orders-group`
- **DLT topic**: `order-create.DLT`

---

### Useful experiments

- Increase `count` and watch lag grow/shrink.
- Change `spring.kafka.listener.concurrency` and compare lag.
- Change the simulated processing delay and see how quickly lag accumulates.

