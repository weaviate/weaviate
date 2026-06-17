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

package schema

import (
	"fmt"
	"reflect"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/entities/models"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	dynamicent "github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
	hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// dangerousVectorConfigChange returns a non-empty reason when the update would rewrite a
// vector's on-disk structure (index type, distance, quantizer) or add/remove a named vector.
// Returns "" for query-time-only changes (ef, flatSearchCutoff, rescoreLimit, ...).
func dangerousVectorConfigChange(old, u *models.Class) string {
	if r := vecDelta("", old.VectorIndexConfig, u.VectorIndexConfig); r != "" {
		return r
	}
	for name := range u.VectorConfig {
		if _, ok := old.VectorConfig[name]; !ok {
			return fmt.Sprintf("named vector %q added", name)
		}
	}
	for name, oldVec := range old.VectorConfig {
		newVec, ok := u.VectorConfig[name]
		if !ok {
			return fmt.Sprintf("named vector %q removed", name)
		}
		if r := vecDelta(name, oldVec.VectorIndexConfig, newVec.VectorIndexConfig); r != "" {
			return r
		}
	}
	return ""
}

// vecDelta reports a structural change between one vector's old and new parsed index config.
//
// Detection fails CLOSED: rather than enumerate the DANGEROUS fields (which silently lets any
// future structural field through), we zero the known QUERY-TIME-SAFE fields on a copy of each
// side and reflect.DeepEqual the remainder. Any field not classified safe survives zeroing, so a
// change to it surfaces as a mismatch and blocks the move. An unknown concrete config type also
// blocks. Test_vectorConfigClassificationComplete forces every leaf to be consciously classified.
func vecDelta(name string, oldCfg, newCfg any) string {
	if oldCfg == nil || newCfg == nil {
		// add/remove is the caller's map check; nil here just means absent on one side
		return ""
	}
	label := name
	if label == "" {
		label = "(legacy vector)"
	}
	o, ok1 := oldCfg.(schemaConfig.VectorIndexConfig)
	n, ok2 := newCfg.(schemaConfig.VectorIndexConfig)
	if !ok1 || !ok2 {
		return fmt.Sprintf("structural vector config change on %s", label)
	}
	// Explicit messages for the two most common structural changes; the generic DeepEqual below
	// would also catch them (an index-type change yields different concrete types), but a precise
	// reason is more useful to the operator.
	if o.IndexType() != n.IndexType() {
		return fmt.Sprintf("vector index type change on %s", label)
	}
	if o.DistanceName() != n.DistanceName() {
		return fmt.Sprintf("distance change on %s", label)
	}
	no, normOk1 := normalizeVectorConfig(oldCfg)
	nn, normOk2 := normalizeVectorConfig(newCfg)
	if !normOk1 || !normOk2 {
		// Unknown config type (e.g. the experimental hfresh index, whose fields we don't yet
		// classify): we can't isolate its structural from its query-time fields, so fail closed on
		// any real change — but a no-op update changes nothing on disk and must pass.
		if reflect.DeepEqual(oldCfg, newCfg) {
			return ""
		}
		return fmt.Sprintf("structural vector config change on %s", label)
	}
	if !reflect.DeepEqual(no, nn) {
		return fmt.Sprintf("structural vector config change on %s", label)
	}
	return ""
}

// normalizeVectorConfig returns a deep copy of cfg with every QUERY-TIME-SAFE field zeroed, so
// reflect.DeepEqual of two normalized copies differs iff a STRUCTURAL field changed. Config structs
// are scalar-only, so a Go value copy is a deep copy. An unknown concrete type returns ok=false so
// the caller fails closed (blocks the move).
func normalizeVectorConfig(cfg any) (any, bool) {
	switch c := cfg.(type) {
	case hnswent.UserConfig:
		return normalizeHNSW(c), true
	case flatent.UserConfig:
		return normalizeFlat(c), true
	case dynamicent.UserConfig:
		c.HnswUC = normalizeHNSW(c.HnswUC)
		c.FlatUC = normalizeFlat(c.FlatUC)
		// threshold is frozen at index construction so a user changing it does nothing
		c.Threshold = 0
		return c, true
	default:
		return nil, false
	}
}

// normalizeHNSW zeroes the query-time-safe leaves of an HNSW config on a value copy. The remaining
// (structural) leaves — Distance and the quantizer enable/param fields — are preserved for the
// DeepEqual in vecDelta. MaxConnections/EFConstruction/Skip/Multivector.*/Skip+TrackDefault-
// Quantization are classified safe (deferred): see plan.md "Consciously-deferred known gap".
func normalizeHNSW(c hnswent.UserConfig) hnswent.UserConfig {
	c.Skip = false
	c.CleanupIntervalSeconds = 0
	c.MaxConnections = 0
	c.EFConstruction = 0
	c.EF = 0
	c.DynamicEFMin = 0
	c.DynamicEFMax = 0
	c.DynamicEFFactor = 0
	c.VectorCacheMaxObjects = 0
	c.FlatSearchCutoff = 0
	c.FilterStrategy = ""
	c.SkipDefaultQuantization = false
	c.TrackDefaultQuantization = false
	c.SQ.RescoreLimit = 0
	c.RQ.RescoreLimit = 0
	c.Multivector = hnswent.MultivectorConfig{} // every Multivector leaf is safe (deferred)
	return c
}

// normalizeFlat zeroes the query-time-safe leaves of a flat config on a value copy. Quantizer
// Enabled/Bits and Distance stay; RescoreLimit/Cache and VectorCacheMaxObjects are query-time.
func normalizeFlat(c flatent.UserConfig) flatent.UserConfig {
	c.VectorCacheMaxObjects = 0
	c.SkipDefaultQuantization = false
	c.TrackDefaultQuantization = false
	c.PQ.RescoreLimit, c.PQ.Cache = 0, false
	c.BQ.RescoreLimit, c.BQ.Cache = 0, false
	c.SQ.RescoreLimit, c.SQ.Cache = 0, false
	c.RQ.RescoreLimit, c.RQ.Cache = 0, false
	return c
}

func findProp(c *models.Class, name string) *models.Property {
	for _, p := range c.Properties {
		if p.Name == name {
			return p
		}
	}
	return nil
}

// disablesAnyIndex reports whether the update turns off a filterable/searchable/rangeable index.
// Disabling deletes the backing LSM buckets; enabling/adding is safe (target reconciles on load).
func disablesAnyIndex(old, new *models.Property) bool {
	return (inverted.HasFilterableIndex(old) && !inverted.HasFilterableIndex(new)) ||
		(inverted.HasSearchableIndex(old) && !inverted.HasSearchableIndex(new)) ||
		(inverted.HasRangeableIndex(old) && !inverted.HasRangeableIndex(new))
}
