# ADR-001: Message Broker Selection
**Date:** 2026-09-10  
**Status:** Accepted  
**Decision:** Redpanda (Kafka-compatible API)

**Rationale:**
1. **Latency-Critical:** Sub-5ms p99 latency reduces the window for out-of-order events.
2. **Operational Simplicity:** Single C++ binary, no JVM, no ZooKeeper/KRaft complexity.
3. **Tiered Storage Native:** Allows 5-year retention without provisioning petabytes of local SSD.
4. **Drop-in Kafka Compatibility:** Flink's KafkaSource/Sink work without modification.