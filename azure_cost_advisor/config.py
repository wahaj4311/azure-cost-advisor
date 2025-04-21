# --- Configuration Constants ---

# Thresholds
SNAPSHOT_AGE_THRESHOLD_DAYS = 90
LOW_CPU_THRESHOLD_PERCENT = 5
APP_SERVICE_PLAN_LOW_CPU_THRESHOLD_PERCENT = 10 # Flag ASPs averaging below this CPU %
SQL_DB_LOW_DTU_THRESHOLD_PERCENT = 10 # Flag DBs averaging below this DTU %
SQL_VCORE_LOW_CPU_THRESHOLD_PERCENT = 10 # Flag vCore DBs averaging below this CPU %
IDLE_CONNECTION_THRESHOLD_GATEWAY = 5 # Threshold for Application Gateway connections
LOW_CPU_THRESHOLD_WEB_APP = 10 # Threshold for Web App CPU

# Settings
METRIC_LOOKBACK_DAYS = 7
LOG_FILENAME = "cleanup_log.txt"
RETAIL_PRICES_API_ENDPOINT = "https://prices.azure.com/api/retail/prices"
HOURS_PER_MONTH = 730 # Approximate hours for monthly cost estimation

# DISK_SIZE_TO_TIER moved to pricing.py 