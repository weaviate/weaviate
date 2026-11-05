//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package searchparams

import (
	"context"
	"fmt"
)

// MaxEF is the largest ef a single search may request.
// It is far above useful HNSW ef values. The index also refuses to allocate a
// candidate queue larger than the searchable graph, so an accepted override
// cannot reserve more slots than the index already holds.
const MaxEF = 1 << 20

// HNSW holds HNSW-only parameters supplied with a single search.
// A nil pointer, or a nil EF, keeps the collection's vector index config
// (including dynamic ef).
type HNSW struct {
	// EF overrides the configured candidate-list size for this query.
	EF *int `json:"ef,omitempty"`
}

// Validate rejects an explicit ef outside [1, MaxEF]. A nil receiver is valid
// and means "use the index configuration".
func (h *HNSW) Validate() error {
	if h == nil || h.EF == nil {
		return nil
	}
	if *h.EF < 1 || *h.EF > MaxEF {
		return fmt.Errorf("hnsw.ef must be between 1 and %d, got %d", MaxEF, *h.EF)
	}
	return nil
}

type hnswEFContextKey struct{}

// WithHNSWSearch returns a context that carries h.EF for the HNSW index.
// A nil override leaves ctx unchanged. The value is read by the HNSW search
// and by the cluster search payload so a replica applies the same override.
func WithHNSWSearch(ctx context.Context, h *HNSW) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if h == nil || h.EF == nil || *h.EF < 1 {
		return ctx
	}
	return context.WithValue(ctx, hnswEFContextKey{}, *h.EF)
}

// HNSWSearchEF returns the search-time ef stored by WithHNSWSearch.
func HNSWSearchEF(ctx context.Context) (int, bool) {
	if ctx == nil {
		return 0, false
	}
	ef, ok := ctx.Value(hnswEFContextKey{}).(int)
	if !ok || ef < 1 {
		return 0, false
	}
	return ef, true
}

// HNSWSearchEFValue is HNSWSearchEF as a pointer for optional JSON fields.
// The pointed-to int is newly allocated.
func HNSWSearchEFValue(ctx context.Context) *int {
	ef, ok := HNSWSearchEF(ctx)
	if !ok {
		return nil
	}
	return &ef
}
