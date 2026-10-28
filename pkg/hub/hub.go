package hub

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"skystream/pkg/adsb"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024 * 16,
	WriteBufferSize: 1024 * 16,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type BoundingBox [4]float64

func (b BoundingBox) Contains(lat, lon float64) bool {
	minLat, maxLat := b[0], b[1]
	minLon, maxLon := b[2], b[3]

	if lat < minLat || lat > maxLat {
		return false
	}

	if minLon <= maxLon {
		return lon >= minLon && lon <= maxLon
	}
	return lon >= minLon || lon <= maxLon
}

type FilterRule struct {
	ID            string       `json:"id"`
	All           bool         `json:"all,omitempty"`
	BBox          *BoundingBox `json:"bbox,omitempty"`
	Registrations []string     `json:"registrations,omitempty"`
	Flights       []string     `json:"flights,omitempty"`
	AircraftTypes []string     `json:"aircraft_types,omitempty"`
	Hexes         []string     `json:"hexes,omitempty"`

	regSet    map[string]struct{}
	flightSet map[string]struct{}
	typeSet   map[string]struct{}
	hexSet    map[string]struct{}
}

func (f *FilterRule) Compile() {
	if f.BBox != nil {
		if f.BBox[0] > f.BBox[1] {
			f.BBox[0], f.BBox[1] = f.BBox[1], f.BBox[0]
		}
	}
	if len(f.Registrations) > 0 {
		f.regSet = make(map[string]struct{}, len(f.Registrations))
		for _, r := range f.Registrations {
			f.regSet[strings.ToUpper(strings.TrimSpace(r))] = struct{}{}
		}
	}
	if len(f.Flights) > 0 {
		f.flightSet = make(map[string]struct{}, len(f.Flights))
		for _, fl := range f.Flights {
			f.flightSet[strings.ToUpper(strings.TrimSpace(fl))] = struct{}{}
		}
	}
	if len(f.AircraftTypes) > 0 {
		f.typeSet = make(map[string]struct{}, len(f.AircraftTypes))
		for _, t := range f.AircraftTypes {
			f.typeSet[strings.ToUpper(strings.TrimSpace(t))] = struct{}{}
		}
	}
	if len(f.Hexes) > 0 {
		f.hexSet = make(map[string]struct{}, len(f.Hexes))
		for _, h := range f.Hexes {
			f.hexSet[strings.ToLower(strings.TrimSpace(h))] = struct{}{}
		}
	}
}

func (f *FilterRule) Matches(ac *adsb.Aircraft) bool {
	if f.All {
		return true
	}

	if f.BBox != nil {
		if ac.Lat == nil || ac.Lon == nil {
			return false
		}
		if !f.BBox.Contains(*ac.Lat, *ac.Lon) {
			return false
		}
	}

	if len(f.hexSet) > 0 {
		hexLower := strings.ToLower(strings.TrimPrefix(ac.Hex, "~"))
		if _, ok := f.hexSet[hexLower]; !ok {
			return false
		}
	}

	if len(f.regSet) > 0 {
		if ac.Registration == "" {
			return false
		}
		if _, ok := f.regSet[strings.ToUpper(ac.Registration)]; !ok {
			return false
		}
	}

	if len(f.flightSet) > 0 {
		if ac.Flight == "" {
			return false
		}
		matched := false
		upperFlight := strings.ToUpper(ac.Flight)
		if _, ok := f.flightSet[upperFlight]; ok {
			matched = true
		} else {
			for target := range f.flightSet {
				if strings.HasPrefix(upperFlight, target) {
					matched = true
					break
				}
			}
		}
		if !matched {
			return false
		}
	}

	if len(f.typeSet) > 0 {
		if ac.AircraftType == "" {
			return false
		}
		if _, ok := f.typeSet[strings.ToUpper(ac.AircraftType)]; !ok {
			return false
		}
	}

	return f.BBox != nil || len(f.hexSet) > 0 || len(f.regSet) > 0 || len(f.flightSet) > 0 || len(f.typeSet) > 0
}

type ClientMessage struct {
	Action        string       `json:"action"`
	ID            string       `json:"id,omitempty"`
	IDs           []string     `json:"ids,omitempty"`
	Subscriptions []FilterRule `json:"subscriptions,omitempty"`
	All           bool         `json:"all,omitempty"`
	BBox          *BoundingBox `json:"bbox,omitempty"`
	Registrations []string     `json:"registrations,omitempty"`
	Flights       []string     `json:"flights,omitempty"`
	AircraftTypes []string     `json:"aircraft_types,omitempty"`
	Hexes         []string     `json:"hexes,omitempty"`
}

type ServerMessage struct {
	Type          string          `json:"type"`
	Message       string          `json:"message,omitempty"`
	ID            string          `json:"id,omitempty"`
	Subscriptions []FilterRule    `json:"subscriptions,omitempty"`
	Timestamp     float64         `json:"timestamp,omitempty"`
	TotalGlobal   int             `json:"total_global,omitempty"`
	MatchedCount  int             `json:"matched_count,omitempty"`
	Aircraft      []adsb.Aircraft `json:"aircraft,omitempty"`
}

type Client struct {
	hub           *Hub
	conn          *websocket.Conn
	send          chan []byte
	mu            sync.RWMutex
	subscriptions map[string]*FilterRule
	autoSubSeq    int
	closed        bool
}

func newClient(hub *Hub, conn *websocket.Conn) *Client {
	return &Client{
		hub:           hub,
		conn:          conn,
		send:          make(chan []byte, 64),
		subscriptions: make(map[string]*FilterRule),
	}
}

func (c *Client) ReadPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()

	c.conn.SetReadLimit(512 * 1024)
	_ = c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		_ = c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			break
		}

		var req ClientMessage
		if err := json.Unmarshal(message, &req); err != nil {
			c.sendJSON(ServerMessage{
				Type:    "error",
				Message: fmt.Sprintf("invalid json: %v", err),
			})
			continue
		}

		c.handleMessage(&req)
	}
}

func (c *Client) WritePump() {
	ticker := time.NewTicker(30 * time.Second)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				_ = c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			_, _ = w.Write(message)
			if err := w.Close(); err != nil {
				return
			}

		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (c *Client) close() {
	c.mu.Lock()
	if !c.closed {
		c.closed = true
		close(c.send)
	}
	c.mu.Unlock()
}

func (c *Client) sendJSON(msg ServerMessage) {
	bytes, err := json.Marshal(msg)
	if err != nil {
		return
	}
	c.mu.RLock()
	closed := c.closed
	c.mu.RUnlock()
	if closed {
		return
	}

	select {
	case c.send <- bytes:
	default:
	}
}

func (c *Client) handleMessage(req *ClientMessage) {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch strings.ToLower(req.Action) {
	case "subscribe", "sub":
		toAdd := req.Subscriptions
		if len(toAdd) == 0 {
			rule := FilterRule{
				ID:            req.ID,
				All:           req.All,
				BBox:          req.BBox,
				Registrations: req.Registrations,
				Flights:       req.Flights,
				AircraftTypes: req.AircraftTypes,
				Hexes:         req.Hexes,
			}
			toAdd = append(toAdd, rule)
		}

		addedIDs := make([]string, 0, len(toAdd))
		for _, sub := range toAdd {
			if sub.ID == "" {
				c.autoSubSeq++
				sub.ID = fmt.Sprintf("sub-%d", c.autoSubSeq)
			}
			sub.Compile()
			c.subscriptions[sub.ID] = &sub
			addedIDs = append(addedIDs, sub.ID)
		}

		c.sendJSON(ServerMessage{
			Type:          "ack",
			Message:       fmt.Sprintf("subscribed to %d rule(s)", len(addedIDs)),
			ID:            strings.Join(addedIDs, ","),
			Subscriptions: c.listSubscriptionsLocked(),
		})

	case "unsubscribe":
		removed := 0
		if req.ID != "" {
			if _, exists := c.subscriptions[req.ID]; exists {
				delete(c.subscriptions, req.ID)
				removed++
			}
		}
		for _, id := range req.IDs {
			if _, exists := c.subscriptions[id]; exists {
				delete(c.subscriptions, id)
				removed++
			}
		}

		c.sendJSON(ServerMessage{
			Type:          "ack",
			Message:       fmt.Sprintf("unsubscribed %d rule(s)", removed),
			Subscriptions: c.listSubscriptionsLocked(),
		})

	case "unsubscribe_all":
		c.subscriptions = make(map[string]*FilterRule)
		c.sendJSON(ServerMessage{
			Type:          "ack",
			Message:       "cleared all subscriptions",
			Subscriptions: nil,
		})

	case "list":
		c.sendJSON(ServerMessage{
			Type:          "subscriptions_list",
			Subscriptions: c.listSubscriptionsLocked(),
		})

	default:
		c.sendJSON(ServerMessage{
			Type:    "error",
			Message: fmt.Sprintf("unknown action: %q", req.Action),
		})
	}
}

func (c *Client) listSubscriptionsLocked() []FilterRule {
	list := make([]FilterRule, 0, len(c.subscriptions))
	for _, s := range c.subscriptions {
		list = append(list, *s)
	}
	return list
}

func (c *Client) FilterSnapshot(snap *adsb.GlobeSnapshot) *ServerMessage {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.subscriptions) == 0 {
		return nil
	}

	hasAll := false
	for _, rule := range c.subscriptions {
		if rule.All {
			hasAll = true
			break
		}
	}

	var matched []adsb.Aircraft
	if hasAll {
		matched = snap.Aircraft
	} else {
		for i := range snap.Aircraft {
			ac := &snap.Aircraft[i]
			for _, rule := range c.subscriptions {
				if rule.Matches(ac) {
					matched = append(matched, *ac)
					break
				}
			}
		}
	}

	return &ServerMessage{
		Type:         "aircraft_update",
		Timestamp:    snap.Timestamp,
		TotalGlobal:  snap.TotalGlobal,
		MatchedCount: len(matched),
		Aircraft:     matched,
	}
}

type Hub struct {
	clients    map[*Client]bool
	broadcast  chan *adsb.GlobeSnapshot
	register   chan *Client
	unregister chan *Client
	maxClients int
	mu         sync.RWMutex
}

func NewHub(maxClients int) *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		broadcast:  make(chan *adsb.GlobeSnapshot, 4),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		maxClients: maxClients,
	}
}

func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				client.close()
			}
			h.mu.Unlock()

		case snap := <-h.broadcast:
			h.mu.RLock()
			clients := make([]*Client, 0, len(h.clients))
			for c := range h.clients {
				clients = append(clients, c)
			}
			h.mu.RUnlock()

			for _, client := range clients {
				msg := client.FilterSnapshot(snap)
				if msg != nil {
					client.sendJSON(*msg)
				}
			}
		}
	}
}

func (h *Hub) Broadcast(snap *adsb.GlobeSnapshot) {
	select {
	case h.broadcast <- snap:
	default:
	}
}

func (h *Hub) ClientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

func (h *Hub) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	if h.maxClients > 0 && h.ClientCount() >= h.maxClients {
		http.Error(w, "Too many connections", http.StatusServiceUnavailable)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("ws upgrade error: %v", err)
		return
	}
	client := newClient(h, conn)
	h.register <- client

	go client.WritePump()
	go client.ReadPump()
}
