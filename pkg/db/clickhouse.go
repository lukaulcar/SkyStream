package db

import (
	"context"
	"fmt"
	"log"
	"math"
	"regexp"
	"strings"
	"time"

	"skystream/pkg/adsb"
	"skystream/pkg/config"

	"github.com/ClickHouse/clickhouse-go/v2"
)

type DB struct {
	conn   clickhouse.Conn
	dbName string
}

const CreateTableQuery = `
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
`

var validDBName = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

func InitDB(ctx context.Context, cfg *config.Config) (*DB, error) {
	if !validDBName.MatchString(cfg.ClickHouseDB) {
		return nil, fmt.Errorf("invalid database name: %q", cfg.ClickHouseDB)
	}

	conn, err := connectClickHouse(ctx, cfg, cfg.ClickHouseDB)
	if err != nil {
		if strings.HasSuffix(strings.ToLower(err.Error()), "does not exist") {

			log.Printf("Database '%s' not found. Connecting to 'default' to create it...", cfg.ClickHouseDB)

			defaultConn, defErr := connectClickHouse(ctx, cfg, "default")
			if defErr != nil {
				return nil, fmt.Errorf("connect to ClickHouse 'default' db: %w", defErr)
			}

			createDb := func() error {
				defer defaultConn.Close()
				createCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
				defer cancel()
				return defaultConn.Exec(createCtx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", cfg.ClickHouseDB))
			}

			if err = createDb(); err != nil {
				return nil, fmt.Errorf("create database '%s': %w", cfg.ClickHouseDB, err)
			}

			conn, err = connectClickHouse(ctx, cfg, cfg.ClickHouseDB)
		}
		if err != nil {
			return nil, fmt.Errorf("connect to ClickHouse: %w", err)
		}
	}

	db := &DB{
		conn:   conn,
		dbName: cfg.ClickHouseDB,
	}

	if err := db.InitPositionsTable(ctx); err != nil {
		return nil, fmt.Errorf("Warning: failed to ensure adsb_positions table: %v", err)
	}

	return db, nil
}

func connectClickHouse(ctx context.Context, cfg *config.Config, database string) (clickhouse.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{cfg.ClickHouseAddr},
		Auth: clickhouse.Auth{
			Database: database,
			Username: cfg.ClickHouseUser,
			Password: cfg.ClickHousePass,
		},
		MaxOpenConns: 5,
		MaxIdleConns: 1,
		DialTimeout:  10 * time.Second,
		ReadTimeout:  60 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := conn.Ping(pingCtx); err != nil {
		_ = conn.Close()
		return nil, err
	}

	return conn, nil
}

func (d *DB) InitPositionsTable(ctx context.Context) error {
	if d.conn == nil {
		return fmt.Errorf("database connection is nil")
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	return d.conn.Exec(ctx, CreateTableQuery)
}

func (d *DB) SaveSnapshot(ctx context.Context, snapshot *adsb.GlobeSnapshot, recordTimes ...time.Time) (int, error) {
	if d == nil || d.conn == nil {
		return 0, fmt.Errorf("database not initialized")
	}
	if snapshot == nil || len(snapshot.Aircraft) == 0 {
		return 0, nil
	}

	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		count, err := d.doSaveSnapshot(ctx, snapshot, recordTimes...)
		if err == nil {
			return count, nil
		}
		lastErr = err
		log.Printf("Warning: ClickHouse save attempt %d/3 failed: %v", attempt, err)
		if attempt < 3 {
			pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			_ = d.conn.Ping(pingCtx)
			cancel()

			timer := time.NewTimer(time.Duration(attempt) * time.Second)
			select {
			case <-ctx.Done():
				timer.Stop()
				return 0, ctx.Err()
			case <-timer.C:
			}
		}
	}

	return 0, fmt.Errorf("ClickHouse batch save failed after 3 attempts: %w", lastErr)
}

func (d *DB) doSaveSnapshot(ctx context.Context, snapshot *adsb.GlobeSnapshot, recordTimes ...time.Time) (int, error) {
	var recordTime time.Time
	if len(recordTimes) > 0 && !recordTimes[0].IsZero() {
		recordTime = recordTimes[0].UTC()
	} else if snapshot.Timestamp > 0 {
		recordTime = time.Unix(int64(snapshot.Timestamp), 0).UTC()
	} else {
		recordTime = time.Now().UTC()
	}

	batch, err := d.conn.PrepareBatch(ctx, `
		INSERT INTO adsb_positions (
			timestamp,
			icao,
			registration,
			aircraft_type,
			callsign,
			squawk,
			lat_e5,
			lon_e5,
			alt_div25,
			ground_speed,
			track
		)
	`)
	if err != nil {
		return 0, fmt.Errorf("prepare batch: %w", err)
	}

	count := 0
	for _, ac := range snapshot.Aircraft {
		if ac.Lat == nil || ac.Lon == nil {
			continue
		}
		lat := *ac.Lat
		lon := *ac.Lon
		if lat < -90 || lat > 90 || lon < -180 || lon > 180 {
			continue
		}

		latE5 := int32(math.Round(lat * 100000.0))
		lonE5 := int32(math.Round(lon * 100000.0))
		icao := NormalizeFixedString6(ac.Hex)

		var altDiv25 int16
		if ac.Ground {
			altDiv25 = -1000
		} else if ac.AltBaro != nil {
			val := int(math.Round(float64(*ac.AltBaro) / 25.0))
			if val < -32768 {
				val = -32768
			} else if val > 32767 {
				val = 32767
			}
			altDiv25 = int16(val)
		} else {
			altDiv25 = 0
		}

		var groundSpeed uint16
		if ac.Speed != nil && *ac.Speed > 0 {
			val := int(math.Round(*ac.Speed))
			if val > 65535 {
				val = 65535
			}
			groundSpeed = uint16(val)
		}

		var track uint16
		if ac.Track != nil && *ac.Track >= 0 {
			val := int(math.Round(*ac.Track))
			if val >= 360 {
				val = val % 360
			}
			track = uint16(val)
		}

		err := batch.Append(
			recordTime,
			icao,
			ac.Registration,
			ac.AircraftType,
			ac.Flight,
			ac.Squawk,
			latE5,
			lonE5,
			altDiv25,
			groundSpeed,
			track,
		)
		if err != nil {
			_ = batch.Abort()
			return 0, fmt.Errorf("batch append error: %w", err)
		}
		count++
	}

	if count == 0 {
		_ = batch.Abort()
		return 0, nil
	}

	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("batch send error: %w", err)
	}

	return count, nil
}

func (d *DB) Close() error {
	if d == nil || d.conn == nil {
		return nil
	}
	return d.conn.Close()
}

func NormalizeFixedString6(hex string) string {
	clean := strings.TrimSpace(strings.TrimPrefix(hex, "~"))
	if len(clean) > 6 {
		clean = clean[:6]
	}
	if len(clean) < 6 {
		clean = fmt.Sprintf("%-6s", clean)
	}
	return strings.ToLower(clean)
}
