"""Unit tests for validation/transform and an end-to-end run against SQLite.
Run:  PYTHONPATH=include pytest -q
"""
import shutil
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from bing_pipeline import extract, load, validate
from bing_pipeline.extract import SchemaError, read_raw
from bing_pipeline.transform import transform, transform_file

FIXTURE = Path(__file__).parent / "fixtures" / "sample.csv"


@pytest.fixture
def raw():
    return read_raw(FIXTURE)


# ------------------------------------------------------------------ validation
def test_clean_sample_has_no_rejects(raw):
    assert (validate.find_rejects(raw) == "").all()


def test_bad_rows_are_rejected_with_reasons(raw):
    bad = raw.copy()
    bad.loc[0, "Date"] = "2026-01-11"          # wrong format
    bad.loc[1, "Impressions"] = "-5"           # negative metric
    bad.loc[2, "Ad ID"] = ""                   # missing key
    bad.loc[3, "Spend"] = "abc"                # non numeric
    bad.loc[5] = bad.loc[4]                    # duplicate grain
    reasons = validate.find_rejects(bad)
    assert "unparseable Date" in reasons[0]
    assert "negative Impressions" in reasons[1]
    assert "missing Ad ID" in reasons[2]
    assert "non-numeric Spend" in reasons[3]
    assert reasons[4] == "" and "duplicate" in reasons[5]


def test_run_fails_when_reject_rate_exceeded(raw, tmp_path):
    raw.loc[:20, "Impressions"] = "x"
    raw.to_csv(tmp_path / "raw.csv", index=False)
    with pytest.raises(validate.ValidationError):
        validate.validate(tmp_path / "raw.csv", tmp_path)


def test_missing_column_is_schema_error(raw, tmp_path):
    p = tmp_path / "bad.csv"
    raw.drop(columns=["Spend"]).to_csv(p, index=False)
    with pytest.raises(SchemaError):
        extract.extract(p, tmp_path / "run")


# ------------------------------------------------------------------ transform
def test_transform_types_and_derived_columns(raw):
    raw.loc[0, "Ad title"] = "   "                         # whitespace-only -> NULL
    df = transform(raw)
    assert df["ad_id"].dtype.kind == "i"                      # '[123]' -> 123
    assert set(df["placement"]) <= {"Top", "Other"}
    assert df["date_key"].between(20260101, 20261231).all()
    assert pd.isna(df.loc[0, "ad_title"])
    assert "top_vs_other" not in df.columns


# ------------------------------------------------------------------ end to end (SQLite)
def run_all(engine, source, run_dir):
    meta = extract.extract(source, run_dir)
    rep = validate.validate(meta["landed_file"], run_dir)
    clean = transform_file(rep["validated_file"], run_dir)["clean_file"]
    load.create_schema(engine)
    load.load_staging(engine, clean)
    for step in (load.load_dim_customer, load.load_dim_account, load.load_dim_campaign,
                 load.load_dim_ad_group, load.load_dim_ad, load.load_dim_device,
                 load.load_dim_network, load.load_dim_match_type, load.load_dim_date):
        step(engine, clean)
    load.load_fact(engine)
    return clean, load.run_quality_checks(engine)


def test_end_to_end_and_idempotent(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path/'dw.db'}")
    _, checks = run_all(engine, FIXTURE, tmp_path / "run1")
    assert checks["row_count"]["staging"] == checks["row_count"]["fact"] == 400

    def counts():
        with engine.connect() as c:
            return {t: c.execute(text(f"select count(*) from {t}")).scalar()
                    for t in ("fact_ad_performance", "dim_ad", "dim_campaign", "dim_device")}

    first = counts()
    run_all(engine, FIXTURE, tmp_path / "run2")             # re-run: must not duplicate anything
    assert counts() == first

    load.refresh_views(engine)
    with engine.connect() as c:
        spend_view = c.execute(text("select sum(spend) from vw_campaign_daily")).scalar()
        spend_fact = c.execute(text("select sum(spend) from fact_ad_performance")).scalar()
    assert float(spend_view) == pytest.approx(float(spend_fact))


def test_dimension_scd1_overwrites_changed_attribute(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path/'dw.db'}")
    clean, _ = run_all(engine, FIXTURE, tmp_path / "run1")
    df = pd.read_csv(clean, keep_default_na=False, na_values=[""])
    df["campaign_status"] = "Paused"
    changed = tmp_path / "changed.csv"
    df.to_csv(changed, index=False)
    res = load.load_dim_campaign(engine, changed)
    assert res["updated"] > 0 and res["inserted"] == 0
