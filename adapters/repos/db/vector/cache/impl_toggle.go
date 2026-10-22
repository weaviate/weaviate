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

package cache

import (
	"os"

	"github.com/sirupsen/logrus"
)

// VectorCacheImplEnv selects the compressed-vector cache implementation at
// process level: "sharded" (default) or "arena". Experiment plumbing in the
// spirit of HNSW_ACORN_FILTER_RATIO: it exists so the same persisted index
// can be served under either cache layout for A/B benchmarking, without any
// per-collection config surface. Consulted at cache construction time
// (index load / compressor creation), not per operation.
const VectorCacheImplEnv = "VECTOR_CACHE_IMPL"

// ArenaCacheSelected reports whether VECTOR_CACHE_IMPL selects the arena
// implementation. Unknown values fall back to the sharded default with a
// warning rather than failing startup.
func ArenaCacheSelected(logger logrus.FieldLogger) bool {
	switch v := os.Getenv(VectorCacheImplEnv); v {
	case "", "sharded":
		return false
	case "arena":
		return true
	default:
		if logger != nil {
			logger.WithField("action", "vector_cache_impl").
				Warnf("unknown %s value %q, using sharded", VectorCacheImplEnv, v)
		}
		return false
	}
}
