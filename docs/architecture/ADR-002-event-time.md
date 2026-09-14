# ADR-002: Event-Time Semantics
**Date:** 2026-09-10  
**Status:** Accepted  
**Decision:** Event-Time processing with Bounded Out-of-Orderness Watermarks.

**Rationale:**
Edge devices experience network jitter and clock skew. Anomaly detection must operate on **event time** (when it occurred), not processing time, to prevent false positives.
**Configuration:** `boundedOutOfOrderness = 60s`, `allowedLateness = 30s`.