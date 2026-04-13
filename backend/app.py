"""
NYC Taxi Trip Explorer
=======================================
Data cleaning pipeline, REST API endpoints, and SQLite database management
for the NYC Taxi Trip Duration dataset.
"""
import os
import sys
import csv
import math
import json
import sqlite3
import logging
from datetime import datetime
from flask import Flask, jsonify, request, send_from_directory, g
# ─── Configuration ─────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(BASE_DIR)
DB_PATH = os.path.join(PROJECT_DIR, "database", "nyc_taxi.db")
CSV_PATH = os.path.join(PROJECT_DIR, "data", "train.csv")
FRONTEND_DIR = os.path.join(PROJECT_DIR, "frontend")
LOG_PATH = os.path.join(PROJECT_DIR, "logs", "pipeline.log")
app = Flask(__name__, static_folder=FRONTEND_DIR)
# ─── Logging ───────────────────────────────────────────────────────────────────
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_PATH, mode="w"),
    ],
)
logger = logging.getLogger("pipeline")
# ─── Database Connection ──────────────────────────────────────────────────────
def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA journal_mode=WAL")
        g.db.execute("PRAGMA foreign_keys=ON")
    return g.db
@app.teardown_appcontext
def close_db(exception):
    db = g.pop("db", None)
    if db is not None:
        db.close()