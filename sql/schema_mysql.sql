-- MySQL 8 DDL, generated from include/bing_pipeline/models.py (the pipeline creates these itself).

CREATE TABLE dim_customer (
	customer_key INTEGER NOT NULL AUTO_INCREMENT, 
	customer_id BIGINT NOT NULL, 
	PRIMARY KEY (customer_key), 
	UNIQUE (customer_id)
);

CREATE TABLE dim_date (
	date_key INTEGER NOT NULL, 
	full_date DATE NOT NULL, 
	year INTEGER NOT NULL, 
	quarter INTEGER NOT NULL, 
	month INTEGER NOT NULL, 
	month_name VARCHAR(12) NOT NULL, 
	day_of_month INTEGER NOT NULL, 
	iso_week INTEGER NOT NULL, 
	day_of_week INTEGER NOT NULL, 
	day_name VARCHAR(12) NOT NULL, 
	is_weekend INTEGER NOT NULL, 
	PRIMARY KEY (date_key), 
	UNIQUE (full_date)
);

CREATE TABLE dim_device (
	device_key INTEGER NOT NULL AUTO_INCREMENT, 
	device_type VARCHAR(32) NOT NULL, 
	device_os VARCHAR(32) NOT NULL, 
	PRIMARY KEY (device_key), 
	CONSTRAINT uq_device_nk UNIQUE (device_type, device_os)
);

CREATE TABLE dim_match_type (
	match_type_key INTEGER NOT NULL AUTO_INCREMENT, 
	delivered_match_type VARCHAR(16) NOT NULL, 
	bid_match_type VARCHAR(16) NOT NULL, 
	PRIMARY KEY (match_type_key), 
	CONSTRAINT uq_match_nk UNIQUE (delivered_match_type, bid_match_type)
);

CREATE TABLE dim_network (
	network_key INTEGER NOT NULL AUTO_INCREMENT, 
	network VARCHAR(64) NOT NULL, 
	placement VARCHAR(16) NOT NULL, 
	PRIMARY KEY (network_key), 
	CONSTRAINT uq_network_nk UNIQUE (network, placement)
);

CREATE TABLE stg_ad_performance (
	report_date DATE NOT NULL, 
	date_key INTEGER NOT NULL, 
	customer_id BIGINT NOT NULL, 
	account_number VARCHAR(32) NOT NULL, 
	account_name VARCHAR(255), 
	account_status VARCHAR(32), 
	campaign_name VARCHAR(255) NOT NULL, 
	campaign_status VARCHAR(32), 
	ad_group_id BIGINT NOT NULL, 
	ad_group_name VARCHAR(255), 
	ad_group_status VARCHAR(32), 
	ad_id BIGINT NOT NULL, 
	ad_title TEXT, 
	ad_description TEXT, 
	ad_type VARCHAR(64), 
	ad_status VARCHAR(32), 
	ad_distribution VARCHAR(32), 
	display_url VARCHAR(512), 
	final_url TEXT, 
	destination_url TEXT, 
	tracking_template TEXT, 
	device_type VARCHAR(32) NOT NULL, 
	device_os VARCHAR(32) NOT NULL, 
	network VARCHAR(64) NOT NULL, 
	placement VARCHAR(16) NOT NULL, 
	delivered_match_type VARCHAR(16) NOT NULL, 
	bid_match_type VARCHAR(16) NOT NULL, 
	currency_code VARCHAR(3) NOT NULL, 
	impressions INTEGER NOT NULL, 
	clicks INTEGER NOT NULL, 
	spend NUMERIC(14, 4) NOT NULL, 
	avg_position NUMERIC(8, 2), 
	conversions NUMERIC(12, 2) NOT NULL, 
	assists NUMERIC(12, 2) NOT NULL
);

CREATE INDEX ix_stg_ad ON stg_ad_performance (ad_id);

CREATE INDEX ix_stg_date ON stg_ad_performance (date_key);

CREATE TABLE dim_account (
	account_key INTEGER NOT NULL AUTO_INCREMENT, 
	account_number VARCHAR(32) NOT NULL, 
	account_name VARCHAR(255), 
	account_status VARCHAR(32), 
	customer_key INTEGER NOT NULL, 
	PRIMARY KEY (account_key), 
	UNIQUE (account_number), 
	FOREIGN KEY(customer_key) REFERENCES dim_customer (customer_key)
);

CREATE TABLE dim_campaign (
	campaign_key INTEGER NOT NULL AUTO_INCREMENT, 
	account_key INTEGER NOT NULL, 
	campaign_name VARCHAR(255) NOT NULL, 
	campaign_status VARCHAR(32), 
	PRIMARY KEY (campaign_key), 
	CONSTRAINT uq_campaign_nk UNIQUE (account_key, campaign_name), 
	FOREIGN KEY(account_key) REFERENCES dim_account (account_key)
);

CREATE TABLE dim_ad_group (
	ad_group_key INTEGER NOT NULL AUTO_INCREMENT, 
	ad_group_id BIGINT NOT NULL, 
	ad_group_name VARCHAR(255), 
	ad_group_status VARCHAR(32), 
	campaign_key INTEGER NOT NULL, 
	PRIMARY KEY (ad_group_key), 
	UNIQUE (ad_group_id), 
	FOREIGN KEY(campaign_key) REFERENCES dim_campaign (campaign_key)
);

CREATE TABLE dim_ad (
	ad_key INTEGER NOT NULL AUTO_INCREMENT, 
	ad_id BIGINT NOT NULL, 
	ad_title TEXT, 
	ad_description TEXT, 
	ad_type VARCHAR(64), 
	ad_status VARCHAR(32), 
	ad_distribution VARCHAR(32), 
	display_url VARCHAR(512), 
	final_url TEXT, 
	destination_url TEXT, 
	tracking_template TEXT, 
	ad_group_key INTEGER NOT NULL, 
	PRIMARY KEY (ad_key), 
	UNIQUE (ad_id), 
	FOREIGN KEY(ad_group_key) REFERENCES dim_ad_group (ad_group_key)
);

CREATE TABLE fact_ad_performance (
	date_key INTEGER NOT NULL, 
	ad_key INTEGER NOT NULL, 
	device_key INTEGER NOT NULL, 
	network_key INTEGER NOT NULL, 
	match_type_key INTEGER NOT NULL, 
	ad_group_key INTEGER NOT NULL, 
	campaign_key INTEGER NOT NULL, 
	account_key INTEGER NOT NULL, 
	currency_code VARCHAR(3) NOT NULL, 
	impressions INTEGER NOT NULL, 
	clicks INTEGER NOT NULL, 
	spend NUMERIC(14, 4) NOT NULL, 
	conversions NUMERIC(12, 2) NOT NULL, 
	assists NUMERIC(12, 2) NOT NULL, 
	avg_position NUMERIC(8, 2), 
	loaded_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, 
	PRIMARY KEY (date_key, ad_key, device_key, network_key, match_type_key), 
	FOREIGN KEY(date_key) REFERENCES dim_date (date_key), 
	FOREIGN KEY(ad_key) REFERENCES dim_ad (ad_key), 
	FOREIGN KEY(device_key) REFERENCES dim_device (device_key), 
	FOREIGN KEY(network_key) REFERENCES dim_network (network_key), 
	FOREIGN KEY(match_type_key) REFERENCES dim_match_type (match_type_key), 
	FOREIGN KEY(ad_group_key) REFERENCES dim_ad_group (ad_group_key), 
	FOREIGN KEY(campaign_key) REFERENCES dim_campaign (campaign_key), 
	FOREIGN KEY(account_key) REFERENCES dim_account (account_key)
);

CREATE INDEX ix_fact_account_date ON fact_ad_performance (account_key, date_key);

CREATE INDEX ix_fact_campaign_date ON fact_ad_performance (campaign_key, date_key);
