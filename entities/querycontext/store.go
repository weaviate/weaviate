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

// Package querycontext provides a mechanism to pass query metadata
// (such as the search vector) through the GraphQL resolver call chain
// via context, so it can be included in the top-level response extensions.
package querycontext

import (
	"context"
	"sync"

	"github.com/weaviate/weaviate/entities/models"
)

type contextKey int

const queryVectorStoreKey contextKey = iota

// QueryVectorStore holds the search vector that was used to perform a
// vector search. It is stored in the request context so that it can be
// retrieved after query execution and returned in the GraphQL extensions field.
type QueryVectorStore struct {
	mu     sync.RWMutex
	vector models.Vector
}

// Set stores the search vector. Safe for concurrent use.
func (s *QueryVectorStore) Set(v models.Vector) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.vector = v
}

// Get retrieves the stored search vector, or nil if none was set.
func (s *QueryVectorStore) Get() models.Vector {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.vector
}

// NewContext injects a new QueryVectorStore into the provided context.
func NewContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, queryVectorStoreKey, &QueryVectorStore{})
}

// FromContext retrieves the QueryVectorStore from the context.
// Returns (store, true) if present, (nil, false) otherwise.
func FromContext(ctx context.Context) (*QueryVectorStore, bool) {
	s, ok := ctx.Value(queryVectorStoreKey).(*QueryVectorStore)
	return s, ok
}
