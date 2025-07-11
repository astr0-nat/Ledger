import sys
from datetime import datetime
import pytz


def pdt_time(*args):
    return datetime.now(pytz.timezone("America/Los_Angeles")).timetuple()


LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": "%(asctime)s - %(levelname)s - %(name)s - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S %Z",
            "converter": pdt_time,
        },
        "access": {
            "format": "%(asctime)s - %(levelname)s - %(name)s - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S %Z",
            "converter": pdt_time,
        },
        "utility_fees": {
            "format": "%(asctime)s - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S %Z",
            "converter": pdt_time,
        },
        "costs_sync": {
            "format": "%(asctime)s - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S %Z",
            "converter": pdt_time,
        }
    },
    "handlers": {
        "default": {
            "level": "INFO",
            "formatter": "standard",
            "class": "logging.StreamHandler",
            "stream": sys.stdout,
        },
        "utility_fees_file": {
                    "level": "INFO",
                    "formatter": "utility_fees",
                    "class": "logging.handlers.RotatingFileHandler",
                    "filename": "logs/failed_utility_fees.log",
                    "maxBytes": 10485760,  # 10MB
                    "backupCount": 5,
                    "encoding": "utf8"
                },
        "costs_sync_file": {
            "level": "INFO",
            "formatter": "costs_sync",
            "class": "logging.handlers.RotatingFileHandler",
            "filename": "logs/failed_costs_sync.log",
            "maxBytes": 10485760,  # 10MB
            "backupCount": 5,
            "encoding": "utf8"
        }
    },
    "loggers": {
        "": {  # Root logger
            "handlers": ["default"],
            "level": "INFO",
            "propagate": True,
        },
        "uvicorn": {  # Uvicorn main logger
            "handlers": ["default"],
            "level": "INFO",
            "propagate": False,
        },
        "uvicorn.error": {  # Uvicorn error logger
            "handlers": ["default"],
            "level": "INFO",
            "propagate": False,
        },
        "uvicorn.access": {  # Uvicorn access logger
            "handlers": ["default"],
            "level": "INFO",
            "propagate": False,
            "formatter": "access",
        },
        "utility_fees": {  # New logger for utility fees
                    "handlers": ["utility_fees_file"],
                    "level": "INFO",
                    "propagate": False,
        },
        "costs_sync": {
            "handlers": ["costs_sync_file"],
            "level": "INFO",
            "propagate": False,
        }
    },
}
