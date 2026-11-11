-- Analytics layer. Plain, dialect-neutral SQL so it runs on MySQL 8 (and SQLite in tests).
-- Ratios are computed HERE, from summed additive measures, never averaged from row-level ratios.
-- Statements are separated by a line containing only ';;' (see load.refresh_views).

DROP VIEW IF EXISTS vw_ad_performance;
;;
-- Flat, fully-joined row-level view: the one place an Insights Agent needs to read from.
CREATE VIEW vw_ad_performance AS
SELECT
    d.full_date, d.year, d.month, d.iso_week, d.day_name, d.is_weekend,
    cu.customer_id,
    ac.account_number, ac.account_name,
    ca.campaign_name, ca.campaign_status,
    ag.ad_group_id, ag.ad_group_name,
    ad.ad_id, ad.ad_title, ad.ad_type, ad.final_url,
    dv.device_type, dv.device_os,
    nw.network, nw.placement,
    mt.delivered_match_type, mt.bid_match_type,
    f.currency_code,
    f.impressions, f.clicks, f.spend, f.conversions, f.assists, f.avg_position
FROM fact_ad_performance f
JOIN dim_date       d  ON d.date_key        = f.date_key
JOIN dim_ad         ad ON ad.ad_key         = f.ad_key
JOIN dim_ad_group   ag ON ag.ad_group_key   = f.ad_group_key
JOIN dim_campaign   ca ON ca.campaign_key   = f.campaign_key
JOIN dim_account    ac ON ac.account_key    = f.account_key
JOIN dim_customer   cu ON cu.customer_key   = ac.customer_key
JOIN dim_device     dv ON dv.device_key     = f.device_key
JOIN dim_network    nw ON nw.network_key    = f.network_key
JOIN dim_match_type mt ON mt.match_type_key = f.match_type_key;
;;
DROP VIEW IF EXISTS vw_campaign_daily;
;;
-- Campaign x day KPIs. avg_position is impression-weighted (a plain AVG would be wrong).
CREATE VIEW vw_campaign_daily AS
SELECT
    d.full_date,
    ac.account_number, ac.account_name,
    ca.campaign_name, ca.campaign_status,
    f.currency_code,
    SUM(f.impressions)                                             AS impressions,
    SUM(f.clicks)                                                  AS clicks,
    SUM(f.spend)                                                   AS spend,
    SUM(f.conversions)                                             AS conversions,
    SUM(f.clicks)      * 1.0 / NULLIF(SUM(f.impressions), 0)       AS ctr,
    SUM(f.spend)       * 1.0 / NULLIF(SUM(f.clicks), 0)            AS cpc,
    SUM(f.spend)       * 1.0 / NULLIF(SUM(f.conversions), 0)       AS cost_per_conversion,
    SUM(f.conversions) * 1.0 / NULLIF(SUM(f.clicks), 0)            AS conversion_rate,
    SUM(f.avg_position * f.impressions) * 1.0
        / NULLIF(SUM(CASE WHEN f.avg_position IS NOT NULL THEN f.impressions END), 0) AS avg_position
FROM fact_ad_performance f
JOIN dim_date     d  ON d.date_key     = f.date_key
JOIN dim_campaign ca ON ca.campaign_key = f.campaign_key
JOIN dim_account  ac ON ac.account_key  = f.account_key
GROUP BY d.full_date, ac.account_number, ac.account_name, ca.campaign_name, ca.campaign_status, f.currency_code;
