# config/settings.py
import os
from pathlib import Path

# Project paths
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
MAPPINGS_DIR = DATA_DIR / "mappings"
TEMP_DIR = DATA_DIR / "temp"  # For extracted zip files

# App settings
APP_TITLE = "Financial Term Mapper"
APP_ICON = "🏦"
MAX_FILE_SIZE = 500  # MB (increased for zip files)

# Supported file types
SUPPORTED_ARCHIVE_TYPES = ['zip']
SUPPORTED_JSON_TYPES = ['json']

# Statement types
STATEMENT_TYPES = ["income_statement", "balance_sheet", "cashflow_statement"]
