# SkyStream

Lightweight Go service that ingests real-time global flight data from ADS-B Exchange (`binCraft` + `zstd`), serves filtered live updates over WebSockets, and archives position snapshots into ClickHouse.

---

## Architecture

```
                      ┌────────────────────────────────┐
                      │    ADS-B Exchange API          │
                      │  (binCraft + zstd whole globe) │
                      └───────────────┬────────────────┘
                                      │  HTTP / 1s interval
                                      ▼
                      ┌────────────────────────────────┐
                      │      SkyStream Poller          │
                      │   (Zstandard decompression &   │
                      │    binary stride parsing)      │
                      └───────┬────────────────┬───────┘
                               │                │
            Dynamic Subscriptions              │ Wall-Clock Scheduled
            (Bounding Box, Callsign, etc.)     │ Batch Ingestion (e.g. 1m)
                               │                │
                               ▼                ▼
                      ┌────────────────┐ ┌────────────────┐
                      │ WebSocket Hub  │ │ ClickHouse DB  │
                      │  (:8080/ws)    │ │ (DoubleDelta + │
                      │                │ │  ZSTD Codecs)  │
                      └────────────────┘ └────────────────┘
```

A single poller loop pulls and decodes the global binary feed, then fans out to both the WebSocket hub and ClickHouse writer. Clients get real-time streams without hammering the upstream API.

---

## Highlights

- **Fast binary ingestion**: Downloads and unpacks ADS-B Exchange's compressed feed (`binCraft`), decoding ~15,000+ aircraft in just a few milliseconds.
- **Client-filtered WebSockets**: Clients can subscribe to geographic bounding boxes (with antimeridian / ±180° rollover handling), airline callsign prefixes (`BAW`, `DLH`), aircraft types, specific hex codes, or the raw firehose.
- **Tiny ClickHouse footprint**: Packs flight positions down to ~3.2 bytes per row via integer scaling and delta codecs. A full year of 1-minute global snapshots (~7.8B rows) fits in ~25 GB.
- **Zero-drift batch commits**: ClickHouse inserts run on clean wall-clock minute marks (`:00`, `:01`, etc.).
- **Standalone modules**: Toggle WebSockets or ClickHouse archiving on or off in `.env`.

---

## Quick Start

### Prerequisites
- Go 1.22+
- ClickHouse (optional, only needed for historical storage)

### Running

```bash
git clone https://github.com/lukaulcar/SkyStream.git
cd SkyStream

cp .env.example .env

go run .
```

Verify it's up:
```bash
curl http://localhost:8080/status
```

---

## Configuration

Set via `.env` or system environment variables:

| Variable | Default | Description |
| :--- | :--- | :--- |
| `PORT` | `8080` | HTTP port for `/status` and `/ws` |
| `ADSB_ENDPOINT` | `https://globe.adsbexchange.com/...` | ADS-B Exchange binCraft API endpoint |
| `ENABLE_WEBSOCKET` | `true` | Enable WebSocket server |
| `WEBSOCKET_REFRESH_INTERVAL` | `1s` | Polling and broadcast interval for WebSockets |
| `ENABLE_CLICKHOUSE` | `true` | Enable periodic database archiving |
| `CLICKHOUSE_ADDR` | `localhost:9000` | ClickHouse native TCP port |
| `CLICKHOUSE_DB` | `skystream` | Target database name |
| `CLICKHOUSE_USER` | `default` | Database user |
| `CLICKHOUSE_PASSWORD` | *(empty)* | Database password |
| `CLICKHOUSE_SAVE_INTERVAL` | `1m` | How often to batch save snapshots |

---

## ClickHouse Storage

SkyStream automatically creates the database and table on startup if missing.

### Table Schema

```sql
CREATE TABLE IF NOT EXISTS adsb_positions
(
    timestamp DateTime CODEC(DoubleDelta, ZSTD(3)),
    icao FixedString(6) CODEC(ZSTD(3)),
    registration LowCardinality(String) CODEC(ZSTD(3)),
    aircraft_type LowCardinality(String) CODEC(ZSTD(3)),
    callsign LowCardinality(String) CODEC(ZSTD(3)),
    squawk LowCardinality(String) CODEC(ZSTD(3)),
    lat_e5 Int32 CODEC(DoubleDelta, ZSTD(3)),
    lon_e5 Int32 CODEC(DoubleDelta, ZSTD(3)),
    alt_div25 Int16 CODEC(Delta(2), ZSTD(3)),
    ground_speed UInt16 CODEC(Delta(2), ZSTD(3)),
    track UInt16 CODEC(Delta(2), ZSTD(3)),
    lat Float64 ALIAS lat_e5 / 100000.0,
    lon Float64 ALIAS lon_e5 / 100000.0,
    altitude_ft Int32 ALIAS if(alt_div25 = -1000, 0, alt_div25 * 25),
    on_ground UInt8 ALIAS if(alt_div25 = -1000, 1, 0),
    INDEX idx_ts timestamp TYPE minmax GRANULARITY 4
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (icao, timestamp)
SETTINGS index_granularity = 8192;
```

### Why this schema? (~48 GB / year)

Logging global flight traffic every minute (`CLICKHOUSE_SAVE_INTERVAL=1m`) with an average of 15,000 aircraft in the air yields:
- 15,000 planes × 1,440 minutes/day × 365 days = **~7.88 billion rows per year**.

Storing raw uncompressed rows would burn hundreds of gigabytes in months. With this schema and ClickHouse column codecs, a full year takes **~48 GB on disk** (~6.05 bytes per aircraft position).

Here is how it's compressed:

1. **`ORDER BY (icao, timestamp)`**:
   ClickHouse stores data sorted on disk. Grouping records by aircraft first keeps consecutive rows tracking the same flight minute-to-minute. Smooth physical motion yields tiny deltas between data points.

2. **Delta and DoubleDelta codecs**:
   - `timestamp` (`DoubleDelta, ZSTD(3)`): At steady 1-minute intervals, the delta between timestamps is almost always 60s. The second delta (the change in deltas) is mostly `0`, compressing to nearly nothing.
   - `lat_e5` & `lon_e5` (`Int32 CODEC(DoubleDelta, ZSTD(3))`): Lat/lon are multiplied by 100,000 and stored as integers rather than 64-bit floats. One minute of flight creates small, predictable integer changes that DoubleDelta collapses easily.
   - `alt_div25` (`Int16 CODEC(Delta(2), ZSTD(3))`): ADS-B broadcasts altitude in 25-foot increments anyway. Storing altitude divided by 25 fits neatly into a 2-byte signed int (`Int16`), using `-1000` for planes on ground. This cuts raw size in half over a standard 32-bit int.
   - `ground_speed` & `track` (`UInt16 CODEC(Delta(2), ZSTD(3))`): 16-bit integers with delta compression.

3. **Dictionary encoding for text**:
   - `registration`, `aircraft_type`, `callsign`, and `squawk` use `LowCardinality(String)`. ClickHouse swaps repetitive strings for small numeric dictionary IDs.
   - `icao` uses `FixedString(6)` to drop string length overhead.

4. **Zero-overhead virtual columns (`ALIAS`)**:
   - `lat`, `lon`, `altitude_ft`, and `on_ground` are `ALIAS` fields.
   - They use **0 bytes of disk space**, but allow standard queries (`SELECT lat, lon, altitude_ft ...`) without needing manual conversions in your application code.

---

## WebSocket API

Connect to:
```
ws://localhost:8080/ws
```

### 1. Bounding Box Filter
Coordinates: `[minLat, maxLat, minLon, maxLon]`. Automatically handles crossing the international date line / antimeridian (`minLon > maxLon`).

```json
{
  "action": "subscribe",
  "id": "central-europe",
  "bbox": [45.0, 55.0, 5.0, 20.0]
}
```

### 2. Airline Callsign or Aircraft Type
```json
{
  "action": "subscribe",
  "id": "flag-carriers",
  "flights": ["BAW", "DLH", "AFR"],
  "aircraft_types": ["A388", "B744", "B748"]
}
```

### 3. Specific ICAO Hex Codes
```json
{
  "action": "subscribe",
  "id": "tracked-planes",
  "hexes": ["4b1802", "3c65c4", "a1234b"]
}
```

### 4. Full Firehose (Everything)
```json
{
  "action": "subscribe",
  "id": "all-traffic",
  "all": true
}
```

### Unsubscribing

Remove a single subscription by ID:
```json
{
  "action": "unsubscribe",
  "id": "central-europe"
}
```

Or clear all subscriptions on your connection:
```json
{
  "action": "unsubscribe_all"
}
```

### Broadcast Message Format

Whenever the poller fetches new data, matching aircraft are pushed to the client:

```json
{
  "type": "aircraft_update",
  "timestamp": 1729168400.125,
  "total_global": 14285,
  "matched_count": 3,
  "aircraft": [
    {
      "hex": "4b1802",
      "flight": "BAW123",
      "registration": "G-EUPJ",
      "aircraft_type": "A319",
      "category": "A1",
      "lat": 51.4700,
      "lon": -0.4543,
      "alt": 32000,
      "ground": false,
      "speed": 455.2,
      "track": 284.1,
      "vert_rate": -64,
      "squawk": "7700",
      "emergency": "general",
      "seen": 0.4
    }
  ]
}
```

---

## Health & Status Endpoint

```bash
GET /status
```

Response:
```json
{
  "status": "ok",
  "uptime_sec": 3612.4,
  "polls_count": 3612,
  "last_fetch": "2026-10-17T12:00:00Z",
  "aircraft_count": 14520,
  "fetch_interval": "1s",
  "websocket": {
    "enabled": true,
    "active_clients": 12
  },
  "clickhouse": {
    "enabled": true,
    "connected": true,
    "database": "skystream",
    "last_save_time": "2026-10-17T12:00:00Z",
    "last_save_count": 14510,
    "total_saved": 870600
  }
}
```

---

## License

[MIT](LICENSE)
