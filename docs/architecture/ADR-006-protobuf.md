# ADR-006: Protobuf Schema Evolution
**Date:** 2026-09-10  
**Status:** Accepted  
**Decision:** Protobuf with strict backward-compatible schema evolution.

**Rationale:**
Minimizes network bandwidth (~5x smaller than JSON) and ensures 10x faster serialization. We will only add fields, never remove or rename them, ensuring old consumers ignore new fields safely.