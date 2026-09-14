# ADR-004: ClickHouse Ingestion
**Date:** 2026-09-10  
**Status:** Accepted  
**Decision:** Idempotent upserts via `ReplacingMergeTree`.

**Rationale:**
ClickHouse does not support true exactly-once ingestion from Flink. We prevent duplicates during checkpoint replays by using `(device_id, event_time, device_sequence_number)` as the primary key for idempotent upserts.