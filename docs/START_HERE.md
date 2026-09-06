# Adaptive Ads: Start Here & Fast-Track Revision Guide

Welcome back! This document is your primary entry point for revising the **Adaptive Ads Data Engineering Platform**. Use the timed pathways below to ramp up quickly based on available revision time.

---

## ⏱️ Choose Your Revision Pathway

### 🚀 If You Have 5 Minutes (Quick Mental Refresh)
Read these 4 core sections to instantly re-anchor on the platform:
1. **[One-Page Cheat Sheet](ONE_PAGE_CHEATSHEET.md)**: The entire platform summarized in a single screen.
2. **[30-Second Elevator Pitch](PITCH.md#30-second-version)**: Quick executive pitch for introductory interview rounds.
3. **[Architecture Overview](ARCHITECTURE_CHEATSHEET.md)**: Core components, technologies, and high-level diagram.
4. **[Key Decisions Matrix](DECISION_CHEATSHEET.md)**: Why Airflow, BigQuery, dbt, and ELT were selected.

---

### ⚡ If You Have 15 Minutes (Core Pipeline & Data Flow)
Read these guides to understand the mechanics of how data moves and transforms:
1. **[Complete Project Story](PROJECT_STORY.md)**: The end-to-end narrative from problem statement to BI dashboards.
2. **[Data Flow & Follow One Event](DATA_FLOW.md)**: Physical trace of a single ad event across code files.
3. **[Data Modeling & Star Schema](REVISION_DATA_MODELING.md)**: Dimensions, facts, surrogate keys, and grains.
4. **[Idempotency & Deduplication](REVISION_IDEMPOTENCY.md)**: Partition-scoped atomic deletes preventing duplicate data.
5. **[SCD Type 2 User Dimension](REVISION_SCD2.md)**: Subscription tier transition modeling with zero gaps.

---

### 🎯 If You Have 30 Minutes (Deep Engineering Mechanics)
Follow this comprehensive technical walkthrough:
1. **[Airflow Orchestration Guide](REVISION_AIRFLOW.md)**: TaskGroups, `EVENT_CONFIG`, dynamic generation, and retries.
2. **[BigQuery Warehouse & Cost Optimization](REVISION_BIGQUERY.md)**: Partitioning, clustering, and incremental bounds.
3. **[dbt Transformations & Incremental Merges](REVISION_DBT.md)**: Staging views, sliding lookbacks, and marts.
4. **[Data Contracts & Quality Pyramid](REVISION_DATA_QUALITY.md)**: Declarative contracts, unit tests, and schema assertions.
5. **[Disaster Recovery & Failure Playbooks](REVISION_FAILURES.md)**: 7 incident scenarios, Time Travel, and backfills.
6. **[What I Actually Coded](WHAT_I_BUILT.md)**: Feature-to-file code reference for fast technical recall.

---

### 🎓 If You Have 60 Minutes (Complete Mastery & Interview Readiness)
Complete the 30-minute path above, then master these interview assets:
1. **[Interview Traps & Edge Cases](INTERVIEW_TRAPS.md)**: How to avoid dangerous interview pitfalls.
2. **[Rapid-Fire Questions (75+ Qs)](INTERVIEW_RAPID_FIRE.md)**: Interactive flashcards with hidden collapsible answers.
3. **[Deep-Dive Technical Questions](INTERVIEW_DEEP_DIVE.md)**: 30+ complex questions with evidence-based answers.
4. **[Scalability & 10x/100x Growth](REVISION_SCALABILITY.md)**: Scaling boundaries, streaming transition, and technical debt.
5. **[Mock Interview Simulation](MOCK_INTERVIEW.md)**: 5-round simulated technical interview script.
6. **[Active Recall Exercises](TEACH_BACK.md)**: 7 whiteboard exercises to test your retention.
7. **[Interview Day Checklist](INTERVIEW_DAY_CHECKLIST.md)**: 15-minute countdown before your call.

