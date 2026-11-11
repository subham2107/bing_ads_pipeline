# Bing Ads Performance Pipeline (Airflow → MySQL star schema)

Parses `BING_MultiDays.csv` (12,497 rows, 11–17 Jan 2026, 3 customers / 9 accounts / 54 campaigns /
234 ad groups / 484 ads) and loads it into a dimensional model built for BI and Insights Agents.

## Run it

```bash
docker compose up -d --build                       # MySQL 8 + Airflow (standalone)
docker compose logs airflow | grep -i password     # admin password printed on first start
# open http://localhost:8080 -> trigger DAG `bing_ads_etl`
```
Warehouse: `mysql -h127.0.0.1 -P3307 -uwarehouse -pwarehouse bing_dw`
(try `SELECT * FROM vw_campaign_daily ORDER BY spend DESC LIMIT 10;`)

Tests (no Docker, uses SQLite): `pip install -r requirements.txt && PYTHONPATH=include pytest -q`

## Pipeline (`dags/bing_ads_etl.py`)

```
extract → validate → transform → create_schema → load_staging ─┐
                                    load_dimensions (TaskGroup)  ├→ load_fact → quality_checks → refresh_views
   customer→account→campaign→ad_group→ad  |  device | network | match_type | date ┘
```
| Task | Responsibility | Failure behaviour |
|---|---|---|
| extract | land a copy of the file, check header against `REQUIRED_COLUMNS` | schema drift fails fast |
| validate | row rules → `02_rejected.csv` with reasons; warnings reported | fails run if reject rate > 1% |
| transform | typing, `[id]` unwrapping, NULL handling, `placement` split | – |
| load_staging | truncate + reload typed staging table | transactional |
| dim_* | upsert on natural key (SCD1), one task each | transactional, retryable |
| load_fact | delete dates in staging, then `INSERT…SELECT` resolving all surrogate keys | transactional |
| quality_checks | staging vs fact: row count, impressions, clicks, spend, conversions | raises on any mismatch |
| refresh_views | recreate analytics views | – |

Data moves between tasks as files in `work/<run_id>/` (only paths/counts go through XCom).
Every step is idempotent, so retrying or re-triggering never duplicates data.
Business logic lives in `include/bing_pipeline/` and is unit-tested without Airflow; the DAG is only wiring.

## Schema (`include/bing_pipeline/models.py`, DDL in `sql/schema_mysql.sql`)

**Fact `fact_ad_performance`** – grain: *date × ad × device(type+OS) × network placement × match types*
(the finest grain in the file; verified unique). Additive measures: impressions, clicks, spend,
conversions, assists. `avg_position` is stored as-is and impression-weighted in views.

**Dimensions:** `dim_date`, `dim_customer → dim_account → dim_campaign → dim_ad_group → dim_ad`,
`dim_device`, `dim_network`, `dim_match_type`.

Key choices
* **Hierarchy as separate dims, with roll-up keys repeated on the fact** (`ad_group/campaign/account_key`):
  clean normalisation but join-free filtering at any level.
* **Campaign has no ID in the export** → identified by (account, name); 54 unique (account, name) pairs;
  the same name (e.g. `Brand`) legitimately appears under several accounts, which is why account is part of the key.
* **Small descriptor dims** (device, network+placement, match types) instead of repeating strings on
  ~12k+ fact rows; `Top vs. other` is split into network + `placement` (Top/Other).
* **Ratios live in views, never the fact** (CTR, CPC, CPA, conversion rate are not summable).
  `vw_campaign_daily` computes them from summed measures; `vw_ad_performance` is a flat joined view
  intended as the single read surface for agents.
* **SCD Type 1**: all ad/campaign attributes are constant in this file. `dim_campaign`/`dim_ad` are the
  SCD2 candidates if status history matters.
* **Currency** is stored on the fact (only USD today) so a second currency can't be silently summed.

## Data findings handled
* UTF-8 **BOM** on the header; IDs wrapped like `[6772783690]`; dates `M/D/YYYY`.
* `Custom Parameters`, `Final Mobile URL`, `Final App URL` are 100% empty and `Language` is constant → dropped.
* `Ad title` is NULL for every *Expanded text ad* (the export omits their title parts) → nullable in `dim_ad`.
* 11 rows have clicks > impressions (partner attribution) → **kept, reported as warning**.
* Rows with 0 impressions can still carry conversions → valid, not rejected.

## Assumptions / limitations
* One file per run, manual trigger. For daily drops set `schedule="@daily"` and template the path.
* Reload is per *date*: re-delivering a day replaces that day. Days absent from a new file are untouched.
* Standalone Airflow + SQLite metadata is for local use; production would use the official multi-service deployment.
* Verified: unit + end-to-end tests on SQLite (SQLAlchemy 1.4/pandas 2.1 *and* 2.x/3.x), full file reconciles
  (12,497 rows; 63,997 impressions; 2,149 clicks; $1,853.16). **Not verified in my environment:** the DAG
  under a live Airflow and the SQL against real MySQL (no Docker there) – DDL was compiled for the MySQL dialect and
  `py_compile`d, so expect to run `docker compose up` once as the real smoke test.
