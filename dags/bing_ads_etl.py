"""Bing Ads performance -> MySQL star schema.

    extract -> validate -> transform -> create_schema -> load_staging
                                                              |
              +--------------------- load_dimensions --------+
              |  customer -> account -> campaign -> ad_group -> ad
              |  device | network | match_type | date          (independent, run in parallel)
              +------------------------------------------------+
                                      |
                              load_fact -> quality_checks -> refresh_views

Why this split: each task has ONE responsibility, a clear input/output file, and can be
retried or re-run on its own. Tasks exchange only small dicts (paths + counts) through XCom;
the data itself travels as files in a per-run work directory, never through the metadata DB.
Business logic lives in include/bing_pipeline/ (unit-testable without Airflow); this file is
only wiring.

Trigger manually (schedule=None); pass {"source_file": "..."} to load a different export.
For a daily drop, change schedule to "@daily" and template the filename with {{ ds }}.
"""
import re
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.utils.task_group import TaskGroup

from bing_pipeline import config, extract as extract_mod, load, transform as transform_mod, validate as validate_mod

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,                          # transient DB/network errors
    "retry_delay": timedelta(minutes=1),
}


def _run_dir() -> Path:
    """Per-run scratch directory, e.g. work/manual__2026-11-10T10_00_00+00_00/"""
    run_id = re.sub(r"[^A-Za-z0-9_.-]", "_", get_current_context()["run_id"])
    path = config.WORK_DIR / run_id
    path.mkdir(parents=True, exist_ok=True)
    return path


@dag(
    dag_id="bing_ads_etl",
    description="Load Bing Ads multi-day export into a star-schema warehouse",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,                     # fact reload is delete+insert: never run two at once
    params={"source_file": str(config.RAW_FILE)},
    default_args=DEFAULT_ARGS,
    tags=["bing", "ads", "warehouse"],
)
def bing_ads_etl():
    @task
    def extract() -> dict:
        source = get_current_context()["params"]["source_file"]
        return extract_mod.extract(Path(source), _run_dir())

    @task
    def validate(meta: dict) -> dict:
        # Raises ValidationError (task fails, no retry value) if reject rate > MAX_REJECT_RATE.
        return validate_mod.validate(meta["landed_file"], _run_dir())

    @task
    def transform(report: dict) -> str:
        return transform_mod.transform_file(report["validated_file"], _run_dir())["clean_file"]

    @task
    def create_schema() -> None:
        load.create_schema(load.get_engine())

    @task
    def load_staging(clean_file: str) -> int:
        return load.load_staging(load.get_engine(), clean_file)

    clean = transform(validate(extract()))
    schema, staged = create_schema(), load_staging(clean)
    schema >> staged

    # ---- dimensions: one task per dimension so failures are isolated and visible in the UI
    def dim_task(task_id, fn):
        @task(task_id=task_id)
        def _load(clean_file: str) -> dict:
            return fn(load.get_engine(), clean_file)
        return _load(clean)

    with TaskGroup("load_dimensions") as dims:
        customer = dim_task("dim_customer", load.load_dim_customer)
        account = dim_task("dim_account", load.load_dim_account)
        campaign = dim_task("dim_campaign", load.load_dim_campaign)
        ad_group = dim_task("dim_ad_group", load.load_dim_ad_group)
        ad = dim_task("dim_ad", load.load_dim_ad)
        customer >> account >> campaign >> ad_group >> ad   # parents before children (FK resolution)
        for tid, fn in [("dim_device", load.load_dim_device), ("dim_network", load.load_dim_network),
                        ("dim_match_type", load.load_dim_match_type), ("dim_date", load.load_dim_date)]:
            dim_task(tid, fn)                               # independent: run in parallel

    @task
    def load_fact() -> dict:
        return load.load_fact(load.get_engine())

    @task
    def quality_checks() -> dict:
        return load.run_quality_checks(load.get_engine())

    @task
    def refresh_views() -> int:
        return load.refresh_views(load.get_engine())

    schema >> dims
    [staged, dims] >> load_fact() >> quality_checks() >> refresh_views()


bing_ads_etl()
