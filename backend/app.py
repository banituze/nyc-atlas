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
# ─── Haversine Distance (no external libraries) ──────────────────────────────
def haversine(lat1, lon1, lat2, lon2):
    """Calculate distance in km between two GPS coordinates."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
# ─── Custom QuickSort (no built-in sort) ──────────────────────────────────────
def quicksort(arr, key_func):
    """
    Custom QuickSort implementation — manual, no built-in sort.
    Uses median-of-three pivot selection for better performance.
    Time Complexity: O(n log n) average, O(n^2) worst case
    Space Complexity: O(log n) for recursion stack
    """
    if len(arr) <= 1:
        return arr
    def _sort(a, lo, hi):
        if lo >= hi:
            return
        mid = (lo + hi) // 2
        if key_func(a[lo]) > key_func(a[mid]):
            a[lo], a[mid] = a[mid], a[lo]
        if key_func(a[lo]) > key_func(a[hi]):
            a[lo], a[hi] = a[hi], a[lo]
        if key_func(a[mid]) > key_func(a[hi]):
            a[mid], a[hi] = a[hi], a[mid]
        a[mid], a[hi] = a[hi], a[mid]
        pivot_val = key_func(a[hi])
        i = lo
        for j in range(lo, hi):
            if key_func(a[j]) <= pivot_val:
                a[i], a[j] = a[j], a[i]
                i += 1
        a[i], a[hi] = a[hi], a[i]
        _sort(a, lo, i - 1)
        _sort(a, i + 1, hi)
    _sort(arr, 0, len(arr) - 1)
    return arr
# ─── Custom Frequency Counter (no Counter/collections) ────────────────────────
def frequency_count(items):
    """
    Manual frequency counter — no collections.Counter.
    Returns dict of {item: count} sorted by count descending (using our quicksort).
    """
    freq = {}
    for item in items:
        if item in freq:
            freq[item] += 1
        else:
            freq[item] = 1
    pairs = [{"key": k, "count": v} for k, v in freq.items()]
    quicksort(pairs, lambda x: -x["count"])
    return pairs
# ─── Zone Classification (grid-based, no external geocoding) ──────────────────
def classify_zone(lat, lon):
    """Classify a GPS point into a named NYC zone using piecewise boundaries."""

    # Manhattan
    if 40.701 <= lat <= 40.882:
        if lat < 40.710: m_east, m_west = -74.000, -74.020
        elif lat < 40.720: m_east, m_west = -73.973, -74.019
        elif lat < 40.732: m_east, m_west = -73.971, -74.015
        elif lat < 40.745: m_east, m_west = -73.972, -74.013
        elif lat < 40.760: m_east, m_west = -73.965, -74.008
        elif lat < 40.775: m_east, m_west = -73.948, -74.000
        elif lat < 40.790: m_east, m_west = -73.935, -73.992
        elif lat < 40.810: m_east, m_west = -73.929, -73.975
        elif lat < 40.835: m_east, m_west = -73.928, -73.958
        elif lat < 40.860: m_east, m_west = -73.920, -73.948
        else:              m_east, m_west = -73.906, -73.932

        if m_west <= lon <= m_east:
            if lat < 40.715:
                return "Lower Manhattan / Financial District"
            elif lat < 40.725:
                return "Tribeca / SoHo" if lon < -74.000 else "Lower East Side / Chinatown"
            elif lat < 40.745:
                if lon < -74.000:
                    return "West Village / Meatpacking"
                elif lon < -73.985:
                    return "East Village / NoHo"
                else:
                    return "Stuyvesant / LES North"
            elif lat < 40.755:
                if lon < -73.998:
                    return "Chelsea / Hudson Yards"
                elif lon < -73.983:
                    return "Midtown South / Flatiron"
                else:
                    return "Gramercy / Murray Hill"
            elif lat < 40.775:
                if lon < -73.981:
                    return "Midtown West / Times Square"
                elif lon < -73.968:
                    return "Midtown East / Grand Central"
                else:
                    return "Upper East Side South"
            elif lat < 40.800:
                return "Upper West Side" if lon < -73.968 else "Upper East Side"
            elif lat < 40.820:
                return "Morningside Heights" if lon < -73.955 else "East Harlem"
            elif lat < 40.840:
                return "Harlem"
            elif lat < 40.867:
                return "Washington Heights"
            else:
                return "Inwood"

    # Bronx
    if 40.785 <= lat <= 40.920 and -73.930 <= lon <= -73.760:
        if lat < 40.830 and lon < -73.900:
            return "South Bronx"
        if lon > -73.870:
            return "East Bronx"
        return "Central Bronx"

    # North Brooklyn
    if 40.700 <= lat < 40.736 and -73.965 <= lon <= -73.900:
        return "North Brooklyn"

    # Brooklyn
    if 40.550 <= lat <= 40.740 and -74.050 <= lon <= -73.855:
        if lat < 40.635:
            return "South Brooklyn"
        elif lon < -73.950:
            return "West Brooklyn"
        else:
            return "Central Brooklyn"

    # Queens
    if 40.540 <= lat <= 40.800 and -73.965 <= lon <= -73.700:
        if lat < 40.680:
            return "South Queens"
        elif lon < -73.880:
            return "West Queens"
        else:
            return "East Queens"

    # Staten Island
    if 40.490 <= lat <= 40.650 and -74.260 <= lon <= -74.050:
        return "Staten Island"

    # Staten Island
    if 40.490 <= lat <= 40.650 and -74.260 <= lon <= -74.050:
        return "Staten Island"

    # Fallback
    return "Outer Boroughs"

def get_period(hour):
    """Classify hour into time period."""
    if 6 <= hour < 12:
        return "morning"
    elif 12 <= hour < 17:
        return "afternoon"
    elif 17 <= hour < 21:
        return "evening"
    else:
        return "night"
# ─── Data Cleaning Pipeline ──────────────────────────────────────
def run_pipeline(db_path=DB_PATH, csv_path=CSV_PATH, limit=None):
    import time as _time
    t_start = _time.time()
    logger.info("=" * 60)
    logger.info("NYC Taxi Trip Data Pipeline — Starting")
    logger.info("=" * 60)
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path, isolation_level=None)
    conn.execute("PRAGMA synchronous = OFF")
    conn.execute("PRAGMA journal_mode = MEMORY")
    conn.execute("PRAGMA temp_store = MEMORY")
    conn.execute("PRAGMA cache_size = -64000")
    conn.execute("PRAGMA locking_mode = EXCLUSIVE")
    conn.executescript(SCHEMA_SQL)
    logger.info(f"Schema created ({_time.time() - t_start:.1f}s)")
    if not os.path.exists(csv_path):
        logger.error(f"CSV not found: {csv_path}")
        conn.close()
        return
    # ── Pre-populate zones (all 31 possible from classify_zone) ──────────
    # This lets us look up zone_id directly during streaming insert, avoiding
    # a second pass over 1.4M rows to link zones.
    ALL_ZONES = [
        "Lower Manhattan / Financial District",
        "Tribeca / SoHo",
        "Lower East Side / Chinatown",
        "West Village / Meatpacking",
        "East Village / NoHo",
        "Stuyvesant / LES North",
        "Chelsea / Hudson Yards",
        "Midtown South / Flatiron",
        "Gramercy / Murray Hill",
        "Midtown West / Times Square",
        "Midtown East / Grand Central",
        "Upper East Side South",
        "Upper West Side",
        "Upper East Side",
        "Morningside Heights",
        "East Harlem",
        "Harlem",
        "Washington Heights",
        "Inwood",
        "South Bronx",
        "Central Bronx",
        "East Bronx",
        "North Brooklyn",
        "South Brooklyn",
        "West Brooklyn",
        "Central Brooklyn",
        "South Queens",
        "West Queens",
        "East Queens",
        "Staten Island",
        "Outer Boroughs",
    ]