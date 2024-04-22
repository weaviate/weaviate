package tenantactivity

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"github.com/weaviate/weaviate/entities/tenantactivity"
)

type Handler struct {
	mu  sync.RWMutex
	src ActivitySource
}

type ActivitySource interface {
	LocalTenantActivity() tenantactivity.ByCollection
}

func NewHandler() *Handler {
	return &Handler{}
}

func (h *Handler) SetSource(source ActivitySource) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.src = source
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h == nil {
		// no tenant handler configured, for example because there is no
		// monitoring activated.
		http.NotFound(w, r)
		return
	}

	h.mu.RLock()
	defer h.mu.RLock()

	if h.src == nil {
		w.Header().Add("retry-after", "30")
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	act := h.src.LocalTenantActivity()

	payload, err := json.Marshal(act)
	if err != nil {
		http.Error(w, fmt.Errorf("encode json: %w", err).Error(), http.StatusInternalServerError)
	}

	w.Header().Add("content-type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(payload)
}
