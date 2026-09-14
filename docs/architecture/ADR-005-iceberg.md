# ADR-005: Iceberg Partitioning
**Date:** 2026-09-10  
**Status:** Accepted  
**Decision:** Hourly partitioning by `event_time` + Flink compaction job.

**Rationale:**
At 100k EPS, Flink writes thousands of small files. Hourly partitions allow skipping 23/24 partitions for "last 24h" queries, and an hourly compaction job merges them into 128MB targets.