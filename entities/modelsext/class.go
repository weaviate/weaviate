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

// DropEpochIDKey carries the drop's generation token inside a dropped entry's
// otherwise-unused (and unparsed) VectorIndexConfig. Minted in the same raft
// commit that sets the "none" marker, immutable for the drop's lifetime, and
// removed with the entry at finalize — so cleanup-task records of a PREVIOUS
// drop of a re-created name can never be mistaken for the current drop's.
const DropEpochIDKey = "dropEpochId"

// DropEpochID returns the dropped entry's generation token, or "" for markers
// written by nodes predating the token (treated as a wildcard by verifiers,
// and as chain-less by coverage inheritance).
func DropEpochID(cfg models.VectorConfig) string {
	m, ok := cfg.VectorIndexConfig.(map[string]any)
	if !ok {
		return ""
	}
	epoch, _ := m[DropEpochIDKey].(string)
	return epoch
}

// DropEpochMatches reports whether a cleanup-task payload epoch may act on a
// marker with the given epoch: an epoch-less marker (older node) accepts any
// payload; a token-bearing marker accepts only its own epoch.
func DropEpochMatches(markerEpoch, payloadEpoch string) bool {
	return markerEpoch == "" || markerEpoch == payloadEpoch
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
