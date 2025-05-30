//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package tenantactivity

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/weaviate/weaviate/entities/tenantactivity"
)

type Handler struct {
	mu  sync.RWMutex
	src ActivitySource
}

type ActivitySource interface {
	LocalTenantActivity(filter tenantactivity.UsageFilter) tenantactivity.ByCollection
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
	defer h.mu.RUnlock()

	if h.src == nil {
		w.Header().Add("retry-after", "30")
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	var filter tenantactivity.UsageFilter
	// parse ?filter from query params
	if filterParam := strings.ToLower(r.URL.Query().Get("filter")); filterParam != "" {
		switch filterParam {
		case "reads", "read", "r":
			filter = tenantactivity.UsageFilterOnlyReads
		case "writes", "write", "w":
			filter = tenantactivity.UsageFilterOnlyWrites
		case "all", "a":
			filter = tenantactivity.UsageFilterAll
		default:
			http.Error(w, fmt.Sprintf("invalid filter: %s", filterParam), http.StatusBadRequest)
			return
		}
	}

	act := h.src.LocalTenantActivity(filter)

	payload, err := json.Marshal(act)
	if err != nil {
		http.Error(w, fmt.Errorf("encode json: %w", err).Error(), http.StatusInternalServerError)
	}

	w.Header().Add("content-type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(payload)
}
