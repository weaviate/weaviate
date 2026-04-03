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
//
// A dropped entry is identified by BOTH an empty VectorIndexType AND a nil
// VectorIndexConfig. Checking both conditions prevents a user-submitted class
// with a missing VectorIndexType (the zero value) from being silently treated
// as dropped and skipping validation. The DeleteClassVectorIndex handler
// explicitly clears both fields when dropping an index.
func IsVectorIndexDropped(cfg models.VectorConfig) bool {
	return cfg.VectorIndexType == "" && cfg.VectorIndexConfig == nil
}

// SetVectorIndexDeletedMarkers iterates over a class's VectorConfig and sets
// the Deleted field to true for any entry whose index has been dropped.
// This ensures backward compatibility for vectors dropped before the Deleted
// field was introduced, as well as consistency for freshly dropped vectors.
// It should be called before returning a class to external API consumers.
func SetVectorIndexDeletedMarkers(class *models.Class) {
	if class == nil {
		return
	}
	for name, cfg := range class.VectorConfig {
		if IsVectorIndexDropped(cfg) && (cfg.Deleted == nil || !*cfg.Deleted) {
			deleted := true
			cfg.Deleted = &deleted
			class.VectorConfig[name] = cfg
		}
	}
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
