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

package db

import (
	"context"
	"fmt"
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
)

// BoostValues reads numeric property values for already-found search results
// straight from columnar buckets, so boost rescoring neither requires the
// boost properties to be part of the requested properties nor pays an object
// unmarshal per candidate.
//
// The returned slice is aligned with results. A result's map misses a
// property when the value cannot be served from a column (docID unknown,
// shard not local, or object has no value) — the caller falls back to the
// result's materialized properties for those. Values are float64 for
// int/number properties and time.Time for dates, matching the types the
// boost scorer consumes.
func (db *DB) BoostValues(ctx context.Context, className, tenant string,
	results []search.Result, props []string,
) ([]map[string]any, error) {
	idx := db.GetIndex(schema.ClassName(className))
	if idx == nil {
		return nil, fmt.Errorf("boost values: index for %q not found", className)
	}

	class := db.schemaGetter.ReadOnlyClass(className)
	if class == nil {
		return nil, fmt.Errorf("boost values: class %q not found", className)
	}

	dataTypes := make(map[string]schema.DataType, len(props))
	for _, propName := range props {
		prop, err := schema.GetPropertyByName(class, propName)
		if err != nil || !inverted.HasColumnarIndex(prop) {
			continue
		}
		dt, _ := schema.AsPrimitive(prop.DataType)
		dataTypes[propName] = dt
	}
	if len(dataTypes) == 0 {
		return make([]map[string]any, len(results)), nil
	}

	out := make([]map[string]any, len(results))

	// Cache shard + bucket resolution per shard name; result sets cluster
	// heavily on few shards. The shard leases are held until every lookup
	// below has completed: the lease is what guarantees the shard (and the
	// mmap'd segments behind the cached bucket pointers) cannot be closed
	// mid-read. Released via the deferred loop.
	type shardBuckets struct {
		buckets map[string]*lsmkv.Bucket
		release func()
	}
	bucketsByShard := map[string]shardBuckets{}
	defer func() {
		for _, sb := range bucketsByShard {
			if sb.release != nil {
				sb.release()
			}
		}
	}()

	for i := range results {
		if results[i].DocID == nil {
			continue
		}
		shardName, err := idx.shardResolver.ResolveShardByObjectID(ctx, results[i].ID, tenant)
		if err != nil {
			continue // fall back to materialized props for this result
		}

		sb, ok := bucketsByShard[shardName]
		if !ok {
			sb = shardBuckets{buckets: map[string]*lsmkv.Bucket{}}
			shard, release, err := idx.GetShard(ctx, shardName)
			if err == nil && shard != nil {
				sb.release = release
				// The readiness gate needs the concrete *Shard; a
				// *LazyLoadShard returned by GetShard is already loaded
				// at this point so unwrapping is cheap.
				concrete, _ := unwrapShard(ctx, shard)
				for propName := range dataTypes {
					// Skip props whose columnar bucket is mid-migration on
					// this shard (enable-columnar backfill in flight): the
					// bucket exists but is incomplete, and serving boost
					// values from it would silently misscore results. The
					// caller falls back to materialized properties.
					if concrete != nil && !concrete.IsColumnarLocallyReady(propName) {
						continue
					}
					b := shard.Store().Bucket(helpers.BucketColumnarFromPropNameLSM(propName))
					if b != nil && b.Strategy() == lsmkv.StrategyColumnar {
						sb.buckets[propName] = b
					}
				}
			}
			bucketsByShard[shardName] = sb
		}
		if len(sb.buckets) == 0 {
			continue // shard not local or buckets missing
		}

		values := make(map[string]any, len(sb.buckets))
		for propName, bucket := range sb.buckets {
			switch dataTypes[propName] {
			case schema.DataTypeNumber:
				if v, ok := bucket.ColumnarLookupFloat64(*results[i].DocID, 0); ok {
					values[propName] = v
				}
			case schema.DataTypeDate:
				if v, ok := bucket.ColumnarLookupInt64(*results[i].DocID, 0); ok {
					values[propName] = time.Unix(0, v).UTC()
				}
			default: // int
				if v, ok := bucket.ColumnarLookupInt64(*results[i].DocID, 0); ok {
					values[propName] = float64(v)
				}
			}
		}
		if len(values) > 0 {
			out[i] = values
		}
	}

	return out, nil
}
