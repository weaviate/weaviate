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

package modelsext

import "github.com/weaviate/weaviate/entities/models"

// DefaultNamedVectorName is a default vector named used to create a named vector or to allow access
// to legacy vector through named vector API.
const DefaultNamedVectorName = "default"

// VectorIndexTypeNone is the VectorIndexType value used to mark a named
// vector whose search index has been dropped but whose vector data still
// exists in the objects bucket. This is mirrored from entities/vectorindex
// to avoid an import cycle.
const VectorIndexTypeNone = "none"

// ClassHasLegacyVectorIndex checks whether there is a legacy index configured on a class.
func ClassHasLegacyVectorIndex(class *models.Class) bool {
	return class.Vectorizer != "" || class.VectorIndexConfig != nil || class.VectorIndexType != ""
}

// ClassGetVectorConfig returns the vector config for a given class and target vector.
// There is a special case for the default vector name, which is used to access the legacy vector.
func ClassGetVectorConfig(class *models.Class, targetVector string) (models.VectorConfig, bool) {
	if cfg, ok := class.VectorConfig[targetVector]; ok {
		return cfg, ok
	}

	if (ClassHasLegacyVectorIndex(class) && targetVector == DefaultNamedVectorName) || targetVector == "" {
		return models.VectorConfig{
			VectorIndexConfig: class.VectorIndexConfig,
			VectorIndexType:   class.VectorIndexType,
			Vectorizer:        class.Vectorizer,
		}, true
	}

	return models.VectorConfig{}, false
}

// IsVectorIndexDropped returns true if the named vector config entry represents
// a dropped index — i.e. the vector data still exists in the objects bucket but
// the search index has been removed from disk.
func IsVectorIndexDropped(cfg models.VectorConfig) bool {
	return cfg.VectorIndexType == VectorIndexTypeNone
}

func ClassUsesVectorisation(class *models.Class) bool {
	needsVectorisation := func(name string) bool {
		return name != "" && name != "none"
	}
	if class == nil {
		return false
	}
	if needsVectorisation(class.Vectorizer) {
		return true
	}
	for _, cfg := range class.VectorConfig {
		vectorizer, ok := cfg.Vectorizer.(map[string]any)
		if !ok {
			continue
		}
		for vectorizerKey := range vectorizer {
			if needsVectorisation(vectorizerKey) {
				return true
			}
		}
	}
	return false
}

// IsVectorlessUpdate reports whether a class update keeps (or lands) a class
// in the vector-less state: the stored class has no legacy vectorizer and no
// live named vectors (either every entry dropped — the flip moment when the
// last drop finalizes — or already none), and the update carries no entries.
// Vector-less classes keep their legacy fields genuinely EMPTY: the update
// body arrives with server defaults filled in (setClassDefaults cannot know
// better), and both the update validator and the RAFT-apply FSM use this
// predicate to ignore those — immutability is relaxed for the comparison,
// and the FSM never copies legacy fields, so nothing synthetic is ever
// stored. Only named-vector classes can reach Vectorizer == "", so a legacy
// class never matches.
func IsVectorlessUpdate(prev, next *models.Class) bool {
	if prev == nil || next == nil {
		return false
	}
	if prev.Vectorizer != "" || len(next.VectorConfig) != 0 {
		return false
	}
	for _, cfg := range prev.VectorConfig {
		if !IsVectorIndexDropped(cfg) {
			return false
		}
	}
	return true
}
