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
		// unknown config type: don't risk a false block
		return ""
	}
	if o.IndexType() != n.IndexType() {
		return fmt.Sprintf("vector index type change on %s", label)
	}
	if o.DistanceName() != n.DistanceName() {
		return fmt.Sprintf("distance change on %s", label)
	}
	if compressionEnabled(oldCfg) != compressionEnabled(newCfg) {
		return fmt.Sprintf("compression toggled on %s", label)
	}
	if compressionParamsChanged(oldCfg, newCfg) {
		return fmt.Sprintf("compression re-parametrized on %s", label)
	}
	return ""
}

// compressionEnabled reports whether any quantizer is enabled. Dynamic recurses: both its flat
// phase (BQ) and post-upgrade HNSW phase write compressed vectors to disk.
func compressionEnabled(cfg any) bool {
	switch c := cfg.(type) {
	case hnswent.UserConfig:
		return c.PQ.Enabled || c.BQ.Enabled || c.SQ.Enabled || c.RQ.Enabled
	case flatent.UserConfig:
		return c.PQ.Enabled || c.BQ.Enabled || c.SQ.Enabled || c.RQ.Enabled
	case dynamicent.UserConfig:
		return compressionEnabled(c.HnswUC) || compressionEnabled(c.FlatUC)
	default:
		return false
	}
}

// compressionParamsChanged reports a quantizer-param change that forces a re-encode of on-disk
// vectors. Query-only knobs (RescoreLimit, Cache) are excluded; toggles are caught by
// compressionEnabled.
func compressionParamsChanged(old, new any) bool {
	switch o := old.(type) {
	case hnswent.UserConfig:
		n, ok := new.(hnswent.UserConfig)
		if !ok {
			return true
		}
		return o.PQ != n.PQ || // PQConfig is wholly structural
			o.SQ.Enabled != n.SQ.Enabled || o.SQ.TrainingLimit != n.SQ.TrainingLimit ||
			o.RQ.Enabled != n.RQ.Enabled || o.RQ.Bits != n.RQ.Bits ||
			o.BQ.Enabled != n.BQ.Enabled
	case flatent.UserConfig:
		n, ok := new.(flatent.UserConfig)
		if !ok {
			return true
		}
		return o.PQ.Enabled != n.PQ.Enabled || o.BQ.Enabled != n.BQ.Enabled ||
			o.SQ.Enabled != n.SQ.Enabled ||
			o.RQ.Enabled != n.RQ.Enabled || o.RQ.Bits != n.RQ.Bits
	case dynamicent.UserConfig:
		n, ok := new.(dynamicent.UserConfig)
		if !ok {
			return true
		}
		return compressionParamsChanged(o.HnswUC, n.HnswUC) || compressionParamsChanged(o.FlatUC, n.FlatUC)
	default:
		return false
	}
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
