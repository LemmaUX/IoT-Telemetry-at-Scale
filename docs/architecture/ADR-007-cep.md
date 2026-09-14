# ADR-007: CEP vs Ordinary Window Aggregation
**Date:** 2026-09-10  
**Status:** Accepted  
**Decision:** Flink CEP for multi-event temporal patterns; ordinary window for single-metric thresholds.

**Rationale:**
CEP state machine clears after a match or timer expiry. This prevents false positives from late data re-triggering the pattern, while ordinary windows remain efficient for simple thresholds.