"""Central configuration. Everything environment-specific comes from env vars
so the same code runs in Docker, locally, and in tests (SQLite)."""
import os
from pathlib import Path

PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).resolve().parents[2]))

RAW_FILE = Path(os.getenv("BING_RAW_FILE", PROJECT_ROOT / "data" / "raw" / "BING_MultiDays.csv"))
WORK_DIR = Path(os.getenv("PIPELINE_WORK_DIR", PROJECT_ROOT / "work"))
SQL_DIR = PROJECT_ROOT / "sql"

# SQLAlchemy URL of the warehouse. MySQL 8 in docker-compose; SQLite is used by the tests.
WAREHOUSE_URL = os.getenv(
    "WAREHOUSE_URL", "mysql+pymysql://warehouse:warehouse@localhost:3306/bing_dw?charset=utf8mb4"
)

# Fail the run if more than this share of rows is rejected by validation.
MAX_REJECT_RATE = float(os.getenv("MAX_REJECT_RATE", "0.01"))

# Columns that must exist in the export. Anything missing = schema drift = hard failure.
REQUIRED_COLUMNS = [
    "Date", "Customer", "Account number", "Account name", "Account status",
    "Campaign name", "Campaign status", "Ad group ID", "Ad group", "Ad group status",
    "Ad ID", "Ad description", "Ad distribution", "Ad status", "Ad title", "Ad type",
    "Tracking Template", "Final URL", "Top vs. other", "Display URL", "Destination URL",
    "Device type", "Device OS", "Delivered match type", "BidMatchType", "Network",
    "Currency code", "Impressions", "Clicks", "Spend", "Avg. position", "Conversions", "Assists",
]
