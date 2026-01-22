//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright (c) 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package traverser

import (
	"context"
	"sync"
)

type queryVectorKey struct{}

const queryVectorDefaultKey = "default"

type QueryVectorHolder struct {
	mu           sync.Mutex
	QueryVectors map[string][]float32
}

func EnsureQueryVectorHolder(ctx context.Context) (context.Context, *QueryVectorHolder) {
	if holder, ok := GetQueryVectorHolder(ctx); ok {
		return ctx, holder
	}

	holder := &QueryVectorHolder{
		QueryVectors: map[string][]float32{},
	}

	return context.WithValue(ctx, queryVectorKey{}, holder), holder
}

func GetQueryVectorHolder(ctx context.Context) (*QueryVectorHolder, bool) {
	if ctx == nil {
		return nil, false
	}

	holder, ok := ctx.Value(queryVectorKey{}).(*QueryVectorHolder)
	if !ok || holder == nil {
		return nil, false
	}

	return holder, true
}

func storeQueryVector(ctx context.Context, vec []float32) {
	holder, ok := GetQueryVectorHolder(ctx)
	if !ok {
		return
	}

	holder.mu.Lock()
	if holder.QueryVectors == nil {
		holder.QueryVectors = map[string][]float32{}
	}
	holder.QueryVectors[queryVectorDefaultKey] = vec
	holder.mu.Unlock()
}
