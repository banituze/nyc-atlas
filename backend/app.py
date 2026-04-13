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
# ─── Database Schema ──────────────────────────────────────────────────────────
SCHEMA_SQL = """
DROP TABLE IF EXISTS trip_flags;
DROP TABLE IF EXISTS trips;
DROP TABLE IF EXISTS time_slots;
DROP TABLE IF EXISTS zones;
DROP TABLE IF EXISTS cleaning_log;
CREATE TABLE zones (
    zone_id INTEGER PRIMARY KEY AUTOINCREMENT,
    zone_name TEXT NOT NULL UNIQUE,
    avg_lat REAL NOT NULL,
    avg_lon REAL NOT NULL,
    trip_count INTEGER DEFAULT 0
);
CREATE TABLE time_slots (
    slot_id INTEGER PRIMARY KEY AUTOINCREMENT,
    hour_of_day INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    period TEXT NOT NULL CHECK(period IN ('morning','afternoon','evening','night')),
    is_weekend INTEGER NOT NULL DEFAULT 0,
    UNIQUE(hour_of_day, day_of_week)
);
"""