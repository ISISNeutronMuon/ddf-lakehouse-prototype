import logging
import os

from celery.schedules import crontab
from flask_caching.backends.filesystemcache import FileSystemCache

logger = logging.getLogger()

#####
# Application setup
# If BASE_PATH is set we are assumed to be proxyed at this prefix location
base_path = os.getenv("BASE_PATH", "")
asset_base = os.getenv("ASSET_BASE_URL") if os.getenv("ASSET_BASE_URL") else base_path
if base_path:
    ENABLE_PROXY_FIX = True
    # Change x_port to 1 if the we are on the same port as the proxy server
    PROXY_FIX_CONFIG = {
        "x_for": 1,
        "x_proto": 1,
        "x_host": 1,
        "x_port": 0,
        "x_prefix": 1,
    }
if asset_base:
    STATIC_ASSETS_PREFIX = asset_base
    APP_ICON = f"{STATIC_ASSETS_PREFIX}/static/assets/images/superset-logo-horiz.png"

#####
# Supersets own database details
SUPERSET_DB_DIALECT = os.getenv("SUPERSET_DB_DIALECT")
SUPERSET_DB_USER = os.getenv("SUPERSET_DB_USER")
SUPERSET_DB_PASSWORD = os.getenv("SUPERSET_DB_PASSWORD")
SUPERSET_DB_HOST = os.getenv("SUPERSET_DB_HOST")
SUPERSET_DB_PORT = os.getenv("SUPERSET_DB_PORT")
SUPERSET_DB_NAME = os.getenv("SUPERSET_DB_NAME")
# The SQLAlchemy connection string.
SQLALCHEMY_DATABASE_URI = (
    f"{SUPERSET_DB_DIALECT}://"
    f"{SUPERSET_DB_USER}:{SUPERSET_DB_PASSWORD}@"
    f"{SUPERSET_DB_HOST}:{SUPERSET_DB_PORT}/{SUPERSET_DB_NAME}"
)
#####

#####
# Caching layer
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = os.getenv("REDIS_PORT", "6379")
REDIS_CELERY_DB = os.getenv("REDIS_CELERY_DB", "0")
REDIS_RESULTS_DB = os.getenv("REDIS_RESULTS_DB", "1")

CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_DB": REDIS_RESULTS_DB,
}
DATA_CACHE_CONFIG = CACHE_CONFIG
#####

#####
# SQL Lab
RESULTS_BACKEND = FileSystemCache("/app/superset_home/sqllab")
SQLLAB_CTAS_NO_LIMIT = True
#####


#####
# Celery
class CeleryConfig:
    broker_url = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_CELERY_DB}"
    imports = ("superset.sql_lab",)
    result_backend = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_RESULTS_DB}"
    worker_prefetch_multiplier = 1
    task_acks_late = False
    beat_schedule = {
        "reports.scheduler": {
            "task": "reports.scheduler",
            "schedule": crontab(minute="*", hour="*"),
        },
        "reports.prune_log": {
            "task": "reports.prune_log",
            "schedule": crontab(minute=10, hour=0),
        },
    }


CELERY_CONFIG = CeleryConfig
#####

#####
# Misc features
FEATURE_FLAGS = {"ALERT_REPORTS": True}
ALERT_REPORTS_NOTIFICATION_DRY_RUN = True
#####
