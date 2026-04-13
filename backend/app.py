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
    zone_id_map = {}
    for zname in ALL_ZONES:
        cur = conn.execute(
            "INSERT INTO zones (zone_name, avg_lat, avg_lon, trip_count) VALUES (?,?,?,?)",
            (zname, 0.0, 0.0, 0)
        )
        zone_id_map[zname] = cur.lastrowid
    zone_stats = {zname: [0.0, 0.0, 0] for zname in ALL_ZONES}
    slot_id_map = {}
    for h in range(24):
        for d in range(7):
            period = ('night' if h < 6 else 'morning' if h < 12
                      else 'afternoon' if h < 17 else 'evening' if h < 21 else 'night')
            is_wk = 1 if d >= 5 else 0
            cur = conn.execute(
                "INSERT INTO time_slots (hour_of_day, day_of_week, period, is_weekend) VALUES (?,?,?,?)",
                (h, d, period, is_wk)
            )
            slot_id_map[(h, d)] = cur.lastrowid
    NYC_LAT_MIN, NYC_LAT_MAX = 40.49, 40.92
    NYC_LON_MIN, NYC_LON_MAX = -74.27, -73.68
    R2 = 12742.0
    PI_180 = 0.017453292519943295
    sin = math.sin
    cos = math.cos
    asin = math.asin
    sqrt = math.sqrt
    _classify_zone = classify_zone
    raw_count = 0
    valid_count = 0
    ex_missing = 0
    ex_coords = 0
    ex_dur_low = 0
    ex_dur_high = 0
    ex_pax = 0
    ex_zero_dist = 0
    ex_far_dist = 0
    ex_speed = 0
    BATCH_SIZE = 10000
    trip_batch = []
    flag_batch = []
    INSERT_TRIP = """INSERT OR IGNORE INTO trips
        (trip_id, vendor_id, pickup_datetime, dropoff_datetime, passenger_count,
         pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude,
         store_and_fwd_flag, trip_duration, distance_km, speed_kmh,
         hour_of_day, day_of_week, month, pickup_zone_id, time_slot_id)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
    INSERT_FLAG = "INSERT INTO trip_flags (trip_id, flag_type, description) VALUES (?,?,?)"
    logger.info("Stage 1/3: Streaming CSV and inserting trips...")
    conn.execute("BEGIN")
    with open(csv_path, "r", buffering=1 << 20) as f:
        first_line = f.readline()
        f.seek(0)
        delim = '\t' if first_line.count('\t') > first_line.count(',') else ','
        logger.info(f"  Detected delimiter: {'TAB' if delim == chr(9) else 'COMMA'}")
        reader = csv.reader(f, delimiter=delim)
        header = next(reader)
        try:
            c_id = header.index("id")
            c_vendor = header.index("vendor_id")
            c_pickup = header.index("pickup_datetime")
            c_dropoff = header.index("dropoff_datetime")
            c_pax = header.index("passenger_count")
            c_plon = header.index("pickup_longitude")
            c_plat = header.index("pickup_latitude")
            c_dlon = header.index("dropoff_longitude")
            c_dlat = header.index("dropoff_latitude")
            c_sfwd = header.index("store_and_fwd_flag")
            c_dur = header.index("trip_duration")
        except ValueError as e:
            logger.error(f"CSV missing required column: {e}")
            conn.rollback()
            conn.close()
            return
        for row in reader:
            raw_count += 1
            if limit and raw_count > limit:
                break
            try:
                trip_id = row[c_id]
                vendor = int(row[c_vendor])
                pickup_dt = row[c_pickup]
                dropoff_dt = row[c_dropoff]
                passengers = int(row[c_pax])
                p_lon = float(row[c_plon])
                p_lat = float(row[c_plat])
                d_lon = float(row[c_dlon])
                d_lat = float(row[c_dlat])
                duration = int(row[c_dur])
            except (IndexError, ValueError):
                ex_missing += 1
                continue
            if (p_lat < NYC_LAT_MIN or p_lat > NYC_LAT_MAX or
                p_lon < NYC_LON_MIN or p_lon > NYC_LON_MAX or
                d_lat < NYC_LAT_MIN or d_lat > NYC_LAT_MAX or
                d_lon < NYC_LON_MIN or d_lon > NYC_LON_MAX):
                ex_coords += 1
                continue
            if duration < 30:
                ex_dur_low += 1
                continue
            if duration > 43200:
                ex_dur_high += 1
                continue
            if passengers < 1 or passengers > 8:
                ex_pax += 1
                continue
            dlat = (d_lat - p_lat) * PI_180
            dlon = (d_lon - p_lon) * PI_180
            a = (sin(dlat * 0.5) ** 2 +
                 cos(p_lat * PI_180) * cos(d_lat * PI_180) * sin(dlon * 0.5) ** 2)
            distance = R2 * asin(sqrt(a))
            if distance < 0.05:
                ex_zero_dist += 1
                continue
            if distance > 100:
                ex_far_dist += 1
                continue
            speed = distance * 3600.0 / duration
            if speed > 150:
                ex_speed += 1
                continue
            try:
                if '/' in pickup_dt:
                    date_part, time_part = pickup_dt.split(' ', 1)
                    m_str, d_str, y_str = date_part.split('/')
                    h_str, mi_str = time_part.split(':', 1)
                    month = int(m_str)
                    day_num = int(d_str)
                    year = int(y_str)
                    hour = int(h_str)
                else:
                    date_part, time_part = pickup_dt.split(' ', 1)
                    year = int(date_part[0:4])
                    month = int(date_part[5:7])
                    day_num = int(date_part[8:10])
                    hour = int(time_part[0:2])
            except (ValueError, IndexError):
                ex_missing += 1
                continue
            if month < 3:
                zm = month + 12
                zy = year - 1
            else:
                zm = month
                zy = year
            K = zy % 100
            J = zy // 100
            zeller = (day_num + (13 * (zm + 1)) // 5 + K + K // 4 + J // 4 + 5 * J) % 7
            dow = (zeller + 5) % 7
            zone_name = _classify_zone(p_lat, p_lon)
            zs = zone_stats[zone_name]
            zs[0] += p_lat
            zs[1] += p_lon
            zs[2] += 1
            zone_id = zone_id_map[zone_name]
            slot_id = slot_id_map.get((hour, dow))
            store_fwd = 1 if row[c_sfwd] == "Y" else 0
            month_val = month
            distance_r = int(distance * 1000 + 0.5) / 1000.0
            speed_r = int(speed * 100 + 0.5) / 100.0
            trip_batch.append((
                trip_id, vendor, pickup_dt, dropoff_dt, passengers,
                p_lon, p_lat, d_lon, d_lat, store_fwd, duration,
                distance_r, speed_r,
                hour, dow, month_val,
                zone_id,
                slot_id
            ))
            if speed > 80:
                flag_batch.append((trip_id, "high_speed", f"Speed: {speed:.1f} km/h"))
            if duration > 7200:
                flag_batch.append((trip_id, "long_trip", f"Duration: {duration // 60} min"))
            if distance > 30:
                flag_batch.append((trip_id, "long_distance", f"Distance: {distance:.1f} km"))
            valid_count += 1
            if len(trip_batch) >= BATCH_SIZE:
                conn.executemany(INSERT_TRIP, trip_batch)
                trip_batch.clear()
                if valid_count % 100000 == 0:
                    elapsed = _time.time() - t_start
                    rate = valid_count / elapsed if elapsed > 0 else 0
                    logger.info(f"  ...{valid_count:,} rows loaded ({rate:,.0f} rows/sec)")
    if trip_batch:
        conn.executemany(INSERT_TRIP, trip_batch)
        trip_batch.clear()
    if flag_batch:
        conn.executemany(INSERT_FLAG, flag_batch)
    t_load = _time.time() - t_start
    logger.info(f"  Raw records: {raw_count:,}")
    logger.info(f"  Valid records: {valid_count:,}")
    excluded = {
        "missing_fields": ex_missing, "invalid_coords": ex_coords,
        "invalid_duration": ex_dur_low, "outlier_duration": ex_dur_high,
        "invalid_passengers": ex_pax, "zero_distance": ex_zero_dist,
        "outlier_distance": ex_far_dist, "outlier_speed": ex_speed,
    }
    for reason, count in excluded.items():
        if count > 0:
            logger.info(f"  Excluded ({reason}): {count:,}")
    logger.info(f"Stage 1 complete in {t_load:.1f}s")
    logger.info("Stage 2/3: Updating zone statistics...")
    t_stage2 = _time.time()
    zone_updates = []
    for zname, (lat_sum, lon_sum, cnt) in zone_stats.items():
        if cnt > 0:
            zone_updates.append((round(lat_sum / cnt, 6), round(lon_sum / cnt, 6), cnt, zone_id_map[zname]))
    conn.executemany(
        "UPDATE zones SET avg_lat=?, avg_lon=?, trip_count=? WHERE zone_id=?",
        zone_updates
    )
    conn.execute("DELETE FROM zones WHERE trip_count = 0")
    logger.info(f"  Active zones: {sum(1 for (_,_,c,_) in zone_updates if c > 0)}")
    logger.info(f"Stage 2 complete in {_time.time() - t_stage2:.1f}s")
    conn.execute(
        "INSERT INTO cleaning_log (stage, records_in, records_out, records_excluded, reason) VALUES (?,?,?,?,?)",
        ("validation", raw_count, valid_count, sum(excluded.values()), json.dumps(excluded))
    )
    conn.execute(
        "INSERT INTO cleaning_log (stage, records_in, records_out, records_excluded, reason) VALUES (?,?,?,?,?)",
        ("insertion", valid_count, valid_count, len(flag_batch), f"{len(flag_batch)} flagged trips")
    )
    conn.commit()
    logger.info("Stage 3/3: Creating indexes...")
    t_idx = _time.time()
    conn.executescript(INDEX_SQL)
    conn.execute("ANALYZE")
    logger.info(f"Indexes created in {_time.time() - t_idx:.1f}s")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.close()
    total = _time.time() - t_start
    logger.info("=" * 60)
    logger.info(f"Pipeline complete in {total:.1f}s  ({valid_count / total:,.0f} rows/sec)")
    logger.info("=" * 60)
# ─── API Endpoints ────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return send_from_directory(FRONTEND_DIR, "index.html")
@app.route("/<path:filename>")
def static_files(filename):
    return send_from_directory(FRONTEND_DIR, filename)
@app.route("/api/stats")
def api_stats():
    db = get_db()
    stats = {}
    row = db.execute("SELECT COUNT(*) as total, AVG(trip_duration) as avg_dur, AVG(distance_km) as avg_dist, AVG(speed_kmh) as avg_speed, SUM(trip_duration) as total_dur FROM trips").fetchone()
    stats["total_trips"] = row["total"]
    stats["avg_duration_min"] = round(row["avg_dur"] / 60, 1) if row["avg_dur"] else 0
    stats["avg_distance_km"] = round(row["avg_dist"], 2) if row["avg_dist"] else 0
    stats["avg_speed_kmh"] = round(row["avg_speed"], 1) if row["avg_speed"] else 0
    stats["total_hours"] = round(row["total_dur"] / 3600, 0) if row["total_dur"] else 0
    row2 = db.execute("SELECT COUNT(*) as cnt FROM trip_flags").fetchone()
    stats["flagged_trips"] = row2["cnt"]
    return jsonify(stats)
@app.route("/api/hourly")
def api_hourly():
    db = get_db()
    rows = db.execute("""
        SELECT hour_of_day, COUNT(*) as count,
               AVG(trip_duration)/60 as avg_duration_min,
               AVG(distance_km) as avg_distance,
               AVG(speed_kmh) as avg_speed
        FROM trips GROUP BY hour_of_day ORDER BY hour_of_day
    """).fetchall()
    return jsonify([dict(r) for r in rows])