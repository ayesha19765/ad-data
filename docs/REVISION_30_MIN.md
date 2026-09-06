# The 30-Minute Interview Revision Plan

Follow this timed 30-minute checklist before stepping into a Data Engineering technical interview.

---

## ⏱️ Minute 00–05: The Pitch & Narrative
- [ ] Read the 60-second elevator pitch in [`docs/PITCH.md`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/docs/PITCH.md).
- [ ] Recall the core problem: unifying ad interactions and video streaming telemetry to calculate accurate ad yield (eCPM/CTR) across user tier changes without misattribution.

## ⏱️ Minute 05–10: End-to-End Architecture & Data Flow
- [ ] Review the architecture matrix in [`docs/ARCHITECTURE_CHEATSHEET.md`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/docs/ARCHITECTURE_CHEATSHEET.md).
- [ ] Trace the 8-step data flow in [`docs/DATA_FLOW.md`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/docs/DATA_FLOW.md) (GCS $\rightarrow$ Airflow TaskGroup $\rightarrow$ BigQuery Staging $\rightarrow$ dbt SCD2/Facts/Marts $\rightarrow$ Looker Studio).

## ⏱️ Minute 10–15: Key Technical Innovations
- [ ] **SCD Type 2**: Review SQL window functions (`LAG`, `LEAD`, running sums) in [`docs/REVISION_SCD2.md`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/docs/REVISION_SCD2.md).
- [ ] **Idempotency**: Review partition-scoped `DELETE + INSERT` and incremental `MERGE` in [`docs/REVISION_IDEMPOTENCY.md`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/docs/REVISION_IDEMPOTENCY.md).
- [ ] **BigQuery Pruning**: Review `incremental_predicates` and projection pruning in [`docs/REVISION_BIGQUERY.md`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/docs/REVISION_BIGQUERY.md).

## ⏱️ Minute 15–20: "Why" & "Why Not" Defenses
- [ ] Review why Kafka/Spark was deferred in [`docs/WHY_NOT.md`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/docs/WHY_NOT.md).
- [ ] Review why MD5 surrogate keys were chosen over sequential IDs in [`docs/WHY.md`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/docs/WHY.md).

## ⏱️ Minute 20–25: Rapid-Fire Flashcards
- [ ] Quiz yourself on 10 random questions in [`docs/INTERVIEW_RAPID_FIRE.md`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/docs/INTERVIEW_RAPID_FIRE.md).
- [ ] Review the 5 trap questions in [`docs/INTERVIEW_TRAPS.md`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/docs/INTERVIEW_TRAPS.md).

## ⏱️ Minute 25–30: The One-Page Cheat Sheet & Whiteboard Prep
- [ ] Scan the single-screen reference in [`docs/ONE_PAGE_CHEATSHEET.md`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/docs/ONE_PAGE_CHEATSHEET.md).
- [ ] Review the whiteboard layout in [`docs/INTERVIEW_DAY_CHECKLIST.md`](file:///Users/ayesha/Downloads/multi-threading-project/ad-data/docs/INTERVIEW_DAY_CHECKLIST.md).

