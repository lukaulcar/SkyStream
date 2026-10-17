package poller

import (
	"context"
	"log"
	"net/http"
	"sync"
	"time"

	"skystream/pkg/adsb"
	"skystream/pkg/config"
	"skystream/pkg/db"
	"skystream/pkg/hub"
)

type WebSocketStatus struct {
	Enabled       bool `json:"enabled"`
	ActiveClients int  `json:"active_clients"`
}

type StatusResponse struct {
	Status        string          `json:"status"`
	UptimeSec     float64         `json:"uptime_sec"`
	PollsCount    uint64          `json:"polls_count"`
	LastFetch     time.Time       `json:"last_fetch,omitempty"`
	AircraftCount int             `json:"aircraft_count"`
	LastError     string          `json:"last_error,omitempty"`
	FetchInterval string          `json:"fetch_interval"`
	WebSocket     WebSocketStatus `json:"websocket"`
	ClickHouse    db.DBStatus     `json:"clickhouse"`
}

type dbSaveJob struct {
	snapshot *adsb.GlobeSnapshot
	bucket   time.Time
}

type Poller struct {
	cfg           *config.Config
	hub           *hub.Hub
	db            *db.DB
	fetchInterval time.Duration
	httpClient    *http.Client
	startTime     time.Time
	mu            sync.RWMutex
	lastFetch     time.Time
	aircraftCount int
	lastError     error
	pollsCount    uint64
	lastDbBucket  time.Time
	dbQueue       chan dbSaveJob
	wg            sync.WaitGroup
}

func NewPoller(cfg *config.Config, h *hub.Hub, d *db.DB) *Poller {
	var interval time.Duration

	if cfg.EnableWebSocket && cfg.EnableClickHouse {
		interval = cfg.WsRefreshInterval
		if cfg.ClickHouseSaveInterval < interval && cfg.ClickHouseSaveInterval > 0 {
			interval = cfg.ClickHouseSaveInterval
		}
	} else if cfg.EnableWebSocket {
		interval = cfg.WsRefreshInterval
	} else if cfg.EnableClickHouse {
		interval = cfg.ClickHouseSaveInterval
	} else {
		interval = 1 * time.Second
	}

	if interval <= 0 {
		interval = 1 * time.Second
	}

	return &Poller{
		cfg:           cfg,
		hub:           h,
		db:            d,
		fetchInterval: interval,
		startTime:     time.Now(),
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		dbQueue: make(chan dbSaveJob, 16),
	}
}

func (p *Poller) Start(ctx context.Context) {
	if !p.cfg.EnableWebSocket && !p.cfg.EnableClickHouse {
		log.Println("Both WebSocket and ClickHouse are disabled. Poller is in idle mode.")
		<-ctx.Done()
		return
	}

	if p.cfg.EnableClickHouse && p.db != nil {
		p.wg.Add(1)
		go p.dbWorker(ctx)
	}

	p.wg.Add(1)
	defer p.wg.Done()

	p.pollOnce(ctx)

	now := time.Now()
	nextTick := now.Truncate(p.fetchInterval).Add(p.fetchInterval)
	timer := time.NewTimer(time.Until(nextTick))
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			p.pollOnce(ctx)

			nextTick = nextTick.Add(p.fetchInterval)
			now := time.Now()
			if now.After(nextTick) {
				nextTick = now.Truncate(p.fetchInterval).Add(p.fetchInterval)
			}
			timer.Reset(time.Until(nextTick))
		}
	}
}

func (p *Poller) Wait() {
	p.wg.Wait()
}

func (p *Poller) pollOnce(ctx context.Context) {
	var snapshot *adsb.GlobeSnapshot
	var err error

	for attempt := 1; attempt <= 2; attempt++ {
		fetchCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
		snapshot, err = adsb.FetchGlobalSnapshot(fetchCtx, p.httpClient, p.cfg.ADSBEndpoint)
		cancel()
		if err == nil {
			break
		}
		if attempt < 2 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(300 * time.Millisecond):
			}
		}
	}

	p.mu.Lock()
	p.pollsCount++
	if err != nil {
		p.lastError = err
		p.mu.Unlock()
		log.Printf("fetch error: %v", err)
		return
	}
	p.lastError = nil
	p.lastFetch = time.Now()
	p.aircraftCount = len(snapshot.Aircraft)
	p.mu.Unlock()

	if p.cfg.EnableWebSocket && p.hub != nil {
		p.hub.Broadcast(snapshot)
	}

	if p.cfg.EnableClickHouse && p.db != nil && p.cfg.ClickHouseSaveInterval > 0 {
		now := time.Now()
		bucket := now.Truncate(p.cfg.ClickHouseSaveInterval)

		p.mu.Lock()
		shouldSave := bucket.After(p.lastDbBucket)
		p.mu.Unlock()

		if shouldSave {
			select {
			case p.dbQueue <- dbSaveJob{snapshot: snapshot, bucket: bucket}:
				p.mu.Lock()
				p.lastDbBucket = bucket
				p.mu.Unlock()
			default:
				log.Printf("Warning: ClickHouse save queue is full (size %d), will retry bucket %s on next tick",
					cap(p.dbQueue), bucket.Format("15:04:00"))
			}
		}
	}
}

func (p *Poller) dbWorker(ctx context.Context) {
	defer p.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-p.dbQueue:
			saveCtx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			savedCount, err := p.db.SaveSnapshot(saveCtx, job.snapshot, job.bucket)
			cancel()
			if err != nil {
				log.Printf("ClickHouse batch save error for bucket %s: %v", job.bucket.Format("15:04:00"), err)
			} else {
				log.Printf("Saved %d positions to ClickHouse for bucket %s (executed at %s)",
					savedCount, job.bucket.Format("15:04:00"), time.Now().Format("15:04:05"))
			}
		}
	}
}

func (p *Poller) GetStatus() StatusResponse {
	p.mu.RLock()
	defer p.mu.RUnlock()

	statusStr := "ok"
	errStr := ""
	if p.lastError != nil {
		errStr = p.lastError.Error()
		if time.Since(p.lastFetch) > 30*time.Second {
			statusStr = "error"
		} else {
			statusStr = "degraded"
		}
	} else if p.lastFetch.IsZero() {
		statusStr = "initializing"
	}

	wsStatus := WebSocketStatus{
		Enabled: p.cfg.EnableWebSocket,
	}
	if p.hub != nil {
		wsStatus.ActiveClients = p.hub.ClientCount()
	}

	var chStatus db.DBStatus
	if p.db != nil {
		chStatus = p.db.GetStatus()
	} else {
		chStatus = db.DBStatus{Enabled: p.cfg.EnableClickHouse}
	}

	return StatusResponse{
		Status:        statusStr,
		UptimeSec:     time.Since(p.startTime).Seconds(),
		PollsCount:    p.pollsCount,
		LastFetch:     p.lastFetch,
		AircraftCount: p.aircraftCount,
		LastError:     errStr,
		FetchInterval: p.fetchInterval.String(),
		WebSocket:     wsStatus,
		ClickHouse:    chStatus,
	}
}
