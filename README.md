# Kafka Spring Boot Demo

![CI](https://github.com/denzilantony/kafka-springboot-demo/actions/workflows/ci.yml/badge.svg)
![Java](https://img.shields.io/badge/Java_21-ED8B00?style=flat&logo=openjdk&logoColor=white)
![Spring Boot](https://img.shields.io/badge/Spring_Boot-6DB33F?style=flat&logo=springboot&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=flat&logo=apachekafka&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green?style=flat)

Event-driven microservices demo using **Apache Kafka** and **Spring Boot 3**.
Demonstrates production-grade messaging patterns used in high-scale 
enterprise platforms — directly transferable from IBM MQ production experience.

---

## Architecture
┌─────────────────────┐     REST POST
│   Order Controller  │──────────────────┐
└─────────────────────┘                  │
▼
┌─────────────────────┐
│  OrderEventProducer  │
│  (Kafka Producer)    │
└──────────┬──────────┘
│
┌──────────▼──────────┐
│   order-events       │
│   (Kafka Topic)      │
│   3 partitions       │
└──────────┬──────────┘
│
┌───────────────┴──────────────┐
│                              │
┌──────────▼──────────┐      ┌───────────▼────────┐
│  OrderEventConsumer  │      │  Dead Letter Queue  │
│  (retry x3 + DLT)   │      │  order-events.DLT  │
└─────────────────────┘      └────────────────────┘
---

## Kafka Patterns Demonstrated

- **Producer/Consumer** — async message publishing and consumption
- **Message Keys** — order ID used as key for partition affinity
- **Retry with Backoff** — 3 retries with exponential backoff
- **Dead Letter Queue (DLT)** — failed messages routed to DLT
- **Topic Partitioning** — 3 partitions for parallel processing
- **Consumer Groups** — independent scaling of consumers

---

## IBM MQ → Kafka Transferability

This project demonstrates patterns directly transferable from
IBM MQ production experience:

| IBM MQ Concept | Kafka Equivalent |
|---|---|
| Queue | Topic partition |
| Message | Kafka Record |
| Producer | KafkaProducer |
| Consumer | KafkaConsumer |
| Dead Letter Queue | DLT Topic |
| Guaranteed delivery | Acknowledgment modes |

---

## Test Coverage
KafkaSpringbootDemoApplicationTests   ✅
Order Event Producer Tests            ✅  3/3 (Embedded Kafka)
Order Event Consumer Tests            ✅  6/6
Order Controller Tests                ✅  3/3
──────────────────────────────────────────
Total                                 ✅ 13/13
**Tests use Spring Embedded Kafka** — no external Kafka needed to run tests.

---

## Tech Stack

- **Java 21** + **Spring Boot 3.x**
- **Apache Kafka** + **Spring Kafka**
- **Embedded Kafka** for testing — no external dependencies
- **JUnit 5** + **Mockito** — TDD approach throughout
- **Docker + Docker Compose** — Kafka, Zookeeper, Kafka UI
- **GitHub Actions** — CI pipeline on every push

---

## Running Locally

### Start Kafka + application
```bash
docker-compose up --build
```

### Access
API:       http://localhost:8080/api/v1/orders
Kafka UI:  http://localhost:8090
Health:    http://localhost:8080/actuator/health

### Run tests (no Kafka needed)
```bash
mvn test
```

### Place an order
```bash
curl -X POST http://localhost:8080/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{
    "orderId": "ORDER-001",
    "customerId": "CUSTOMER-001",
    "productId": "PRODUCT-001",
    "quantity": 2,
    "totalAmount": 59.98,
    "status": "CREATED"
  }'
```

---

## Author

**Denzil Antony** — Senior Java Backend Engineer, Germany
[linkedin.com/in/denzilantony](https://linkedin.com/in/denzilantony)