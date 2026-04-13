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
CREATE TABLE trips (
    trip_id TEXT PRIMARY KEY,
    vendor_id INTEGER NOT NULL,
    pickup_datetime TEXT NOT NULL,
    dropoff_datetime TEXT NOT NULL,
    passenger_count INTEGER NOT NULL,
    pickup_longitude REAL NOT NULL,
    pickup_latitude REAL NOT NULL,
    dropoff_longitude REAL NOT NULL,
    dropoff_latitude REAL NOT NULL,
    store_and_fwd_flag INTEGER NOT NULL DEFAULT 0,
    trip_duration INTEGER NOT NULL,
    -- Derived features
    distance_km REAL NOT NULL,
    speed_kmh REAL NOT NULL,
    hour_of_day INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    month INTEGER NOT NULL,
    -- Foreign keys
    pickup_zone_id INTEGER,
    time_slot_id INTEGER,
    FOREIGN KEY (pickup_zone_id) REFERENCES zones(zone_id),
    FOREIGN KEY (time_slot_id) REFERENCES time_slots(slot_id)
);
CREATE TABLE trip_flags (
    flag_id INTEGER PRIMARY KEY AUTOINCREMENT,
    trip_id TEXT NOT NULL,
    flag_type TEXT NOT NULL,
    description TEXT,
    FOREIGN KEY (trip_id) REFERENCES trips(trip_id) ON DELETE CASCADE
);
CREATE TABLE cleaning_log (
    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
    stage TEXT NOT NULL,
    records_in INTEGER,
    records_out INTEGER,
    records_excluded INTEGER,
    reason TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
"""
# Indexes created AFTER bulk insert for massive speedup
INDEX_SQL = """
CREATE INDEX idx_trips_hour ON trips(hour_of_day);
CREATE INDEX idx_trips_dow ON trips(day_of_week);
CREATE INDEX idx_trips_month ON trips(month);
CREATE INDEX idx_trips_vendor ON trips(vendor_id);
CREATE INDEX idx_trips_duration ON trips(trip_duration);
CREATE INDEX idx_trips_distance ON trips(distance_km);
CREATE INDEX idx_trips_speed ON trips(speed_kmh);
CREATE INDEX idx_trips_passengers ON trips(passenger_count);
CREATE INDEX idx_trips_pickup_zone ON trips(pickup_zone_id);
CREATE INDEX idx_trips_time_slot ON trips(time_slot_id);
CREATE INDEX idx_flags_trip ON trip_flags(trip_id);
CREATE INDEX idx_flags_type ON trip_flags(flag_type);
"""