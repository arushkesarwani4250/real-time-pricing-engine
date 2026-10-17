package downstream

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"sync"

	"real_time/internal/engine"
)

// Hub maintains the set of active downstream clients and broadcasts delta price updates.
type Hub struct {
	mu            sync.RWMutex
	clients       map[*Client]bool
	register      chan *Client
	unregister    chan *Client
	broadcast     chan *engine.MarketPrice
	pricingEngine *engine.PricingEngine
	validSymbols  map[string]bool
}

// NewHub constructs a new Downstream Hub with optional symbol validation.
func NewHub(pricingEngine *engine.PricingEngine, allowedSymbols ...[]string) *Hub {
	h := &Hub{
		clients:       make(map[*Client]bool),
		register:      make(chan *Client),
		unregister:    make(chan *Client),
		broadcast:     make(chan *engine.MarketPrice, 1000),
		pricingEngine: pricingEngine,
		validSymbols:  make(map[string]bool),
	}
	if len(allowedSymbols) > 0 {
		for _, s := range allowedSymbols[0] {
			h.validSymbols[strings.ToUpper(strings.TrimSpace(s))] = true
		}
	}
	return h
}

// ValidateSymbols checks which symbols are known and which are unrecognized.
func (h *Hub) ValidateSymbols(symbols []string) (valid []string, invalid []string) {
	if len(h.validSymbols) == 0 {
		return symbols, nil
	}
	for _, s := range symbols {
		norm := strings.ToUpper(strings.TrimSpace(s))
		if norm == "*" || h.validSymbols[norm] {
			valid = append(valid, norm)
		} else {
			invalid = append(invalid, norm)
		}
	}
	return valid, invalid
}

// Run executes the central event loop of the Hub.
func (h *Hub) Run(ctx context.Context) {
	log.Println("[Hub] Downstream WebSocket Hub active and waiting for client connections...")

	for {
		select {
		case <-ctx.Done():
			h.mu.Lock()
			for client := range h.clients {
				close(client.send)
				delete(h.clients, client)
			}
			h.mu.Unlock()
			return

		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()
			log.Printf("[Hub] Client connected. Total active clients: %d", h.ClientCount())

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
			}
			h.mu.Unlock()
			log.Printf("[Hub] Client disconnected. Total active clients: %d", h.ClientCount())

		case price := <-h.broadcast:
			update := CompactPriceUpdate{
				Type:      "price_update",
				Symbol:    price.Symbol,
				Buy:       price.Buy,
				Sell:      price.Sell,
				Mid:       price.MidPrice,
				High24h:   price.High24h,
				Low24h:    price.Low24h,
				Change24h: price.Change24h,
				Timestamp: price.Timestamp.UnixMilli(),
				LatencyMs: price.LatencyMs,
			}

			data, err := json.Marshal(update)
			if err != nil {
				continue
			}

			// Deliver delta update to subscribed clients
			h.mu.RLock()
			var slow []*Client
			for client := range h.clients {
				if client.IsSubscribed(price.Symbol) {
					select {
					case client.send <- data:
					default:
						slow = append(slow, client)
					}
				}
			}
			h.mu.RUnlock()

			// Evict slow clients that overflowed buffer
			if len(slow) > 0 {
				h.mu.Lock()
				for _, c := range slow {
					if _, ok := h.clients[c]; ok {
						log.Printf("[Hub] Dropping slow client (buffer overflow)")
						close(c.send)
						delete(h.clients, c)
					}
				}
				h.mu.Unlock()
			}
		}
	}
}

// Broadcast dispatches an enriched price record to the hub's broadcast queue.
func (h *Hub) Broadcast(price *engine.MarketPrice) {
	select {
	case h.broadcast <- price:
	default:
	}
}

// GetSnapshot retrieves the current price snapshot of all symbols from the pricing engine.
func (h *Hub) GetSnapshot() []*engine.MarketPrice {
	if h.pricingEngine != nil {
		return h.pricingEngine.GetAllPrices()
	}
	return []*engine.MarketPrice{}
}

// ClientCount returns the count of currently connected clients.
func (h *Hub) ClientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}
