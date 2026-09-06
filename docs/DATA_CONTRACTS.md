# Telemetry Data Contracts & Interface Specifications

## 1. Overview & Governance Principles
Data contracts establish a formal agreement between upstream telemetry producers (web, iOS, Android, CTV players) and the **Adaptive Ads** data platform. They prevent silent schema corruption, enforce semantic data types, and specify quality expectations before data reaches the BigQuery data warehouse.

```
[ Telemetry Producer ] ──► [ Contract Gate (`contracts/*.yml`) ] ──► [ BigQuery Staging ]
```

---

## 2. Event Stream Contracts

### Contract 1: `watch_events` (Video Stream Playback)
- **Logical Owner**: Platform / Video Streaming Engineering
- **Target Table**: `adaptive_ads_stg.watch_events`
- **Data Grain**: One record per video playback interval or milestone event.
- **Partition Key**: `ts` (`TIMESTAMP`, `HOUR` granularity in staging, `DAY` in facts)
- **Required Fields**: `ts`, `userId`, `video`, `duration`, `level`
- **Optional Fields**: `auth`, `city`, `state`, `userAgent`, `lon`, `lat`, `firstName`, `lastName`, `dateOfBirth`, `gender`, `registration`
- **Quality Expectations**:
  - `ts`: ISO-8601 UTC timestamp; must not be null or in the future.
  - `duration`: Float64 ≥ 0.0 seconds.
  - `level`: Enum in `['free', 'paid', 'NA']`.
  - `userId`: Int64 (0 indicates anonymous unauthenticated stream).

---

### Contract 2: `ad_events` (Advertising Interactions)
- **Logical Owner**: Platform / AdTech Engineering
- **Target Table**: `adaptive_ads_stg.ad_events`
- **Data Grain**: One record per ad exposure, impression, or click event.
- **Partition Key**: `ts` (`TIMESTAMP`, `HOUR` granularity)
- **Required Fields**: `ts`, `userId`, `adType`, `video`, `duration`, `level`
- **Optional Fields**: `auth`, `city`, `state`, `userAgent`, `lon`, `lat`, `firstName`, `lastName`, `dateOfBirth`, `gender`, `registration`
- **Quality Expectations**:
  - `adType`: Non-empty string (e.g., `pre-roll`, `mid-roll`, `banner`).
  - `duration`: Float64 ≥ 0.0 seconds.
  - `level`: Enum in `['free', 'paid', 'NA']`.

---

### Contract 3: `page_view_events` (Client Navigation Telemetry)
- **Logical Owner**: Platform / Product Analytics Engineering
- **Target Table**: `adaptive_ads_stg.page_view_events`
- **Data Grain**: One record per web or mobile page transition.
- **Partition Key**: `ts` (`TIMESTAMP`, `HOUR` granularity)
- **Required Fields**: `ts`, `userId`, `page`, `status`
- **Optional Fields**: `auth`, `method`, `level`, `city`, `state`, `userAgent`, `lon`, `lat`, `device`, `os`, `duration`, `video`
- **Quality Expectations**:
  - `page`: Standardized application route string.
  - `status`: HTTP / Application status integer (e.g. 200, 404).

---

### Contract 4: `auth_events` (User Authentication Sessions)
- **Logical Owner**: Platform / Identity & Security Engineering
- **Target Table**: `adaptive_ads_stg.auth_events`
- **Data Grain**: One record per login, logout, or registration attempt.
- **Partition Key**: `ts` (`TIMESTAMP`, `HOUR` granularity)
- **Required Fields**: `ts`, `userId`, `success`, `level`
- **Optional Fields**: `city`, `state`, `userAgent`, `lon`, `lat`, `firstName`, `lastName`, `dateOfBirth`, `gender`, `device`, `os`, `registration`
- **Quality Expectations**:
  - `success`: Boolean indicating authentication outcome.
  - `userId`: Positive integer identifier.

---

## 3. Schema Evolution & Compatibility Rules

| Change Type | Allowed? | Contract Action Required |
| :--- | :--- | :--- |
| **Additive Field** | Yes | Update `contracts/<stream>.yml` with optional field; update `schema.py` |
| **Type Relaxation** | Yes | E.g., `INT64` → `NUMERIC`; update contract and staging views |
| **Field Deprecation** | Yes | Mark field deprecated in contract; apply `COALESCE(col, 'NA')` default in staging |
| **Field Deletion / Type Narrowing** | **Breaking** | Prohibited without major version contract increment |

