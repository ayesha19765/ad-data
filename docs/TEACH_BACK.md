# Active Recall & Teach-Back Exercises

Use these active recall exercises to test your mastery. For maximum learning retention, try explaining each scenario out loud or writing your answer on paper before checking the solution.

---

## Exercise 1: Explain SCD Type 2 Window Functions to a Junior Engineer
**Prompt**: *"Explain how `dim_users.sql` converts a series of raw login/tier events into continuous SCD2 date ranges using `LAG`, `SUM`, and `LEAD`."*

<details>
<summary><b>View Self-Assessment Solution</b></summary>

1. **Step 1 (`LAG`)**: Look at the previous row's `subscription_tier` for that user. If it matches the current tier, flag `isNewState = 0`. If it changed, flag `isNewState = 1`.
2. **Step 2 (`SUM`)**: Compute a cumulative running sum of `isNewState` ordered by timestamp. Every time a user changes tiers, the running sum increments, creating a unique `stateGroup` ID for each continuous period.
3. **Step 3 (`GROUP BY` & `MIN`)**: Group by `userId`, `subscriptionTier`, and `stateGroup`. The earliest event in that group is `MIN(eventTimestamp)`, which becomes `rowActivationDate`.
4. **Step 4 (`LEAD`)**: Use `LEAD(MIN(eventTimestamp))` to find when the next tier began. That timestamp becomes the `rowExpirationDate` for the current record.
5. **Step 5 (Active Flag)**: If `rowExpirationDate` is `NULL`, fill with `'9999-12-31'` and set `isCurrent = TRUE`.
</details>

---

## Exercise 2: Explain Why `incremental_predicates` Matter in BigQuery
**Prompt**: *"Why does dbt's default `merge` strategy perform poorly on large BigQuery tables, and how do `incremental_predicates` solve it?"*

<details>
<summary><b>View Self-Assessment Solution</b></summary>

- **Default Behavior**: When dbt runs `MERGE INTO target USING source ON target.key = source.key`, BigQuery performs a full table scan across all historical partitions of `target` to locate matching keys, resulting in high query latency and costly on-demand byte scan charges.
- **With `incremental_predicates`**: We pass `DBT_INTERNAL_DEST.eventDate >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)`. BigQuery prunes all partitions older than 3 days during the `MERGE` join, scanning only the recent window and slashing scan bytes by up to 90%.
</details>

---

## Exercise 3: Distinguish Pipeline Duplicates from Source Duplicates
**Prompt**: *"What is the difference between a pipeline duplicate and a source duplicate, and how does this repository handle each?"*

<details>
<summary><b>View Self-Assessment Solution</b></summary>

- **Pipeline Duplicates**: Caused by infrastructure retries (e.g. Airflow task retrying halfway through loading). Handled at the **staging ingestion layer** by executing a partition-scoped delete (`DELETE FROM staging WHERE partition_hour = ...`) before inserting Parquet files.
- **Source Duplicates**: Caused by upstream client applications emitting the same event ID twice due to network retry loops. Handled at the **warehouse modeling layer** by dbt incremental `merge` keyed on unique surrogate keys (`adEventKey`), updating or ignoring existing IDs rather than inserting duplicates.
</details>

---

## Exercise 4: Defend "Why Not Kafka?" to a Staff Architect
**Prompt**: *"The staff architect asks: 'Why didn't you build this with Kafka and Flink? All modern ad systems use streaming.' How do you respond?"*

<details>
<summary><b>View Self-Assessment Solution</b></summary>

- **Acknowledge the Context**: Agree that real-time streaming is essential for real-time programmatic ad bidding and fraud detection.
- **Clarify the Business SLA**: State that this platform's SLA is 1-hour analytical reporting, campaign yield analysis, and subscriber engagement aggregation.
- **Compare Cost & Complexity**: Explain that batch loading Parquet from GCS directly into BigQuery via Airflow satisfies the 1-hour SLA with zero idle compute cost, zero cluster management, and no 24/7 broker infrastructure.
- **Provide the Growth Roadmap**: Reference the documented scalability plan in `REVISION_SCALABILITY.md` to introduce Cloud Pub/Sub and Apache Beam if the business requirement changes to sub-minute bidding feedback loops.
</details>

