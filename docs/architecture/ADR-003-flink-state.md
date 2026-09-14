# ADR-003: Flink State Architecture
**Date:** 2026-09-10  
**Status:** Accepted  
**Decision:** RocksDB state backend with incremental checkpoints to MinIO.

**Rationale:**
PulseGrid maintains state for 100k+ devices simultaneously. RocksDB handles TB-scale state off-heap, preventing JVM OOM errors and long GC pauses, while incremental checkpoints reduce duration from minutes to seconds.