"""Warehouse schema (star schema) defined once in SQLAlchemy so the exact same
model creates MySQL DDL in production and SQLite in tests.

Grain of the fact table
-----------------------
One row per  date x ad x device (type+OS) x network placement x match type.
That is the finest grain present in the Bing export, so every coarser report
(campaign/day, account/week, ...) is a plain GROUP BY.

Hierarchy
---------
customer -> account -> campaign -> ad_group -> ad
The fact carries ad/ad_group/campaign/account keys redundantly so analysts and
Insights Agents can filter at any level without chaining joins.

Dimension strategy: SCD Type 1 (overwrite). In this export every ad/campaign
attribute is constant, so history tracking would add cost with no benefit; if
status history becomes a requirement, dim_campaign/dim_ad are the SCD2 candidates.
"""
from sqlalchemy import (BigInteger, Column, Date, DateTime, ForeignKey, Index, Integer,
                        MetaData, Numeric, String, Table, Text, UniqueConstraint, func)

metadata = MetaData()


def _pk(name):
    """Surrogate key: plain INT autoincrement (works on MySQL and SQLite)."""
    return Column(name, Integer, primary_key=True, autoincrement=True)


# --------------------------------------------------------------------------- staging
# Flat, typed, validated copy of the file. Truncated and reloaded on every run.
stg_ad_performance = Table(
    "stg_ad_performance", metadata,
    Column("report_date", Date, nullable=False),
    Column("date_key", Integer, nullable=False),
    Column("customer_id", BigInteger, nullable=False),
    Column("account_number", String(32), nullable=False),
    Column("account_name", String(255)),
    Column("account_status", String(32)),
    Column("campaign_name", String(255), nullable=False),
    Column("campaign_status", String(32)),
    Column("ad_group_id", BigInteger, nullable=False),
    Column("ad_group_name", String(255)),
    Column("ad_group_status", String(32)),
    Column("ad_id", BigInteger, nullable=False),
    Column("ad_title", Text),
    Column("ad_description", Text),
    Column("ad_type", String(64)),
    Column("ad_status", String(32)),
    Column("ad_distribution", String(32)),
    Column("display_url", String(512)),
    Column("final_url", Text),
    Column("destination_url", Text),
    Column("tracking_template", Text),
    Column("device_type", String(32), nullable=False),
    Column("device_os", String(32), nullable=False),
    Column("network", String(64), nullable=False),
    Column("placement", String(16), nullable=False),
    Column("delivered_match_type", String(16), nullable=False),
    Column("bid_match_type", String(16), nullable=False),
    Column("currency_code", String(3), nullable=False),
    Column("impressions", Integer, nullable=False),
    Column("clicks", Integer, nullable=False),
    Column("spend", Numeric(14, 4), nullable=False),
    Column("avg_position", Numeric(8, 2)),
    Column("conversions", Numeric(12, 2), nullable=False),
    Column("assists", Numeric(12, 2), nullable=False),
    Index("ix_stg_ad", "ad_id"),
    Index("ix_stg_date", "date_key"),
)

# --------------------------------------------------------------------------- dimensions
dim_date = Table(
    "dim_date", metadata,
    Column("date_key", Integer, primary_key=True, autoincrement=False),  # YYYYMMDD
    Column("full_date", Date, nullable=False, unique=True),
    Column("year", Integer, nullable=False),
    Column("quarter", Integer, nullable=False),
    Column("month", Integer, nullable=False),
    Column("month_name", String(12), nullable=False),
    Column("day_of_month", Integer, nullable=False),
    Column("iso_week", Integer, nullable=False),
    Column("day_of_week", Integer, nullable=False),  # 1=Mon .. 7=Sun
    Column("day_name", String(12), nullable=False),
    Column("is_weekend", Integer, nullable=False),
)

dim_customer = Table(
    "dim_customer", metadata,
    _pk("customer_key"),
    Column("customer_id", BigInteger, nullable=False, unique=True),
)

dim_account = Table(
    "dim_account", metadata,
    _pk("account_key"),
    Column("account_number", String(32), nullable=False, unique=True),
    Column("account_name", String(255)),
    Column("account_status", String(32)),
    Column("customer_key", Integer, ForeignKey("dim_customer.customer_key"), nullable=False),
)

# The export has no campaign ID, so a campaign is identified by (account, name).
dim_campaign = Table(
    "dim_campaign", metadata,
    _pk("campaign_key"),
    Column("account_key", Integer, ForeignKey("dim_account.account_key"), nullable=False),
    Column("campaign_name", String(255), nullable=False),
    Column("campaign_status", String(32)),
    UniqueConstraint("account_key", "campaign_name", name="uq_campaign_nk"),
)

dim_ad_group = Table(
    "dim_ad_group", metadata,
    _pk("ad_group_key"),
    Column("ad_group_id", BigInteger, nullable=False, unique=True),
    Column("ad_group_name", String(255)),
    Column("ad_group_status", String(32)),
    Column("campaign_key", Integer, ForeignKey("dim_campaign.campaign_key"), nullable=False),
)

dim_ad = Table(
    "dim_ad", metadata,
    _pk("ad_key"),
    Column("ad_id", BigInteger, nullable=False, unique=True),
    Column("ad_title", Text),  # NULL for Expanded Text Ads: the export omits their title parts
    Column("ad_description", Text),
    Column("ad_type", String(64)),
    Column("ad_status", String(32)),
    Column("ad_distribution", String(32)),
    Column("display_url", String(512)),
    Column("final_url", Text),
    Column("destination_url", Text),
    Column("tracking_template", Text),
    Column("ad_group_key", Integer, ForeignKey("dim_ad_group.ad_group_key"), nullable=False),
)

# Small "junk-style" dimensions: low-cardinality descriptors that would otherwise
# bloat the fact table with repeated strings.
dim_device = Table(
    "dim_device", metadata,
    _pk("device_key"),
    Column("device_type", String(32), nullable=False),
    Column("device_os", String(32), nullable=False),
    UniqueConstraint("device_type", "device_os", name="uq_device_nk"),
)

dim_network = Table(
    "dim_network", metadata,
    _pk("network_key"),
    Column("network", String(64), nullable=False),     # e.g. 'Bing and Yahoo! search'
    Column("placement", String(16), nullable=False),   # 'Top' | 'Other' (split from 'Top vs. other')
    UniqueConstraint("network", "placement", name="uq_network_nk"),
)

dim_match_type = Table(
    "dim_match_type", metadata,
    _pk("match_type_key"),
    Column("delivered_match_type", String(16), nullable=False),  # what the query actually matched
    Column("bid_match_type", String(16), nullable=False),        # what the advertiser bid on
    UniqueConstraint("delivered_match_type", "bid_match_type", name="uq_match_nk"),
)

# --------------------------------------------------------------------------- fact
fact_ad_performance = Table(
    "fact_ad_performance", metadata,
    Column("date_key", Integer, ForeignKey("dim_date.date_key"), primary_key=True),
    Column("ad_key", Integer, ForeignKey("dim_ad.ad_key"), primary_key=True),
    Column("device_key", Integer, ForeignKey("dim_device.device_key"), primary_key=True),
    Column("network_key", Integer, ForeignKey("dim_network.network_key"), primary_key=True),
    Column("match_type_key", Integer, ForeignKey("dim_match_type.match_type_key"), primary_key=True),
    # Redundant (derivable from ad_key) rollup keys, for join-free filtering.
    Column("ad_group_key", Integer, ForeignKey("dim_ad_group.ad_group_key"), nullable=False),
    Column("campaign_key", Integer, ForeignKey("dim_campaign.campaign_key"), nullable=False),
    Column("account_key", Integer, ForeignKey("dim_account.account_key"), nullable=False),
    Column("currency_code", String(3), nullable=False),
    # Additive measures only. Ratios (CTR, CPC, ...) live in views because they are not summable.
    Column("impressions", Integer, nullable=False),
    Column("clicks", Integer, nullable=False),
    Column("spend", Numeric(14, 4), nullable=False),
    Column("conversions", Numeric(12, 2), nullable=False),
    Column("assists", Numeric(12, 2), nullable=False),
    # Not additive: must be impression-weighted when rolled up (done in the views).
    Column("avg_position", Numeric(8, 2)),
    Column("loaded_at", DateTime, nullable=False, server_default=func.current_timestamp()),
    Index("ix_fact_campaign_date", "campaign_key", "date_key"),
    Index("ix_fact_account_date", "account_key", "date_key"),
)
