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
// process level: "sharded" (default) or "arena". The arena ships as an
// opt-in for this release; the default stays sharded. There is no
// per-collection config surface: the choice applies to every compressed
// index of the process and is consulted at cache construction time (index
// load / compressor creation), not per operation.
//
// Caveats for "arena", both observable through
// weaviate_vector_cache_arena_slab_bytes against
// weaviate_vector_cache_arena_live_vectors:
//
//   - Churn: deleted records free their slot but not their chunk (no chunk
//     reclamation yet), and vector updates move to fresh doc ids, so under
//     update/delete churn arena memory follows the id range touched since
//     startup rather than the live vectors, e.g. 2M RQ8 1536d objects
//     updated 5x between restarts hold ~16 GB vs ~3.9 GB sharded. A restart
//     refills only the codes still in the compressed bucket (deleted ones
//     leave it at tombstone cleanup), so it releases every chunk that no
//     longer holds one; dropping the cache releases all of them.
//   - Small indexes: chunks hold 4096 records and are allocated whole, so an
//     index below roughly 3,600 vectors uses more memory than with the
//     sharded cache. With many small indexes, as in multi-tenant
//     collections, this dominates: 10,000 tenants of 100 RQ8 1536d vectors
//     are ~65 GB arena vs ~1.8 GB sharded.
//
// The arena suits read-heavy, append-mostly workloads with large indexes.
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
