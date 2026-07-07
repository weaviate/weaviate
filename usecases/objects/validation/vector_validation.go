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

package validation

import (
	"context"
	"fmt"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modelsext"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
)

func (v *Validator) vector(ctx context.Context, class *models.Class,
	incomingObject *models.Object,
) error {
	// In case the schema has a legacy vector index, we need to check if there is a default named vector which
	// is used to transport legacy vector via the named vector API.
	if defaultVec, ok := incomingObject.Vectors[modelsext.DefaultNamedVectorName]; ok && modelsext.ClassHasLegacyVectorIndex(class) {
		vec, ok := defaultVec.([]float32)
		if !ok {
			return fmt.Errorf("vector %s has to be a float32 array", modelsext.DefaultNamedVectorName)
		}
		incomingObject.Vector = vec
		delete(incomingObject.Vectors, modelsext.DefaultNamedVectorName)
	}

	if !modelsext.ClassHasLegacyVectorIndex(class) && len(incomingObject.Vector) > 0 {
		// if there is only one named vector we can assume that the single vector
		if len(class.VectorConfig) == 1 {
			namedVectorName := ""
			for key := range class.VectorConfig {
				namedVectorName = key
			}
			var vector []float32
			if len(incomingObject.Vector) > 0 {
				vector = make([]float32, len(incomingObject.Vector))
				copy(vector, incomingObject.Vector)
			}
			incomingObject.Vectors = map[string]models.Vector{namedVectorName: models.Vector(vector)}
			incomingObject.Vector = nil
			return nil
		}

		return fmt.Errorf("collection %v configuration does not have single vector index", class.Class)
	}

	var incomingTargetVectors []string
	for name := range incomingObject.Vectors {
		_, ok := class.VectorConfig[name]
		if !ok {
			return fmt.Errorf("collection %v does not have configuration for vector %s", class.Class, name)
		}

		incomingTargetVectors = append(incomingTargetVectors, name)
	}

	if len(class.VectorConfig) == 0 && len(incomingTargetVectors) > 0 {
		return fmt.Errorf("collection %v is configured without multiple named vectors, but received named vectors: %v", class.Class, incomingTargetVectors)
	}

	return v.vectorShapes(class, incomingObject)
}

// vectorShapes validates each provided named vector against the shape the
// schema expects (single vector vs multi-vector) and normalizes empty
// vectors. Shape mismatches are rejected here, before anything is persisted:
// letting a single vector through to a multi-vector index would initialize
// the index with the wrong dimensionality and corrupt every subsequent
// multi-vector insert.
func (v *Validator) vectorShapes(class *models.Class, incomingObject *models.Object) error {
	for name, vec := range incomingObject.Vectors {
		cfg, ok := class.VectorConfig[name]
		if !ok {
			// unknown names were already rejected by the caller
			continue
		}
		indexCfg, ok := cfg.VectorIndexConfig.(schemaConfig.VectorIndexConfig)
		if !ok {
			// the config was not parsed into a typed form; leave shape
			// validation to the vector index layer
			continue
		}
		isMultiVector := indexCfg.IsMultiVector()

		if outerVectorLen(vec) == 0 {
			// An empty vector historically means "no vector provided": single
			// vector targets treat it as a no-op, and targets with a
			// vectorizer module vectorize as if the vector was omitted. Only
			// a self-provided multi-vector target rejects it, as there is
			// nothing to fall back to and no valid empty multi-vector exists.
			if isMultiVector && targetVectorizer(class, name) == "none" {
				return fmt.Errorf("vector %q in collection %q: multi-vector cannot be empty", name, class.Class)
			}
			delete(incomingObject.Vectors, name)
			continue
		}

		multiShaped := isMultiVectorShaped(vec)
		if isMultiVector && !multiShaped {
			return fmt.Errorf("vector %q in collection %q is configured as multi-vector: expected an array of token vectors, got a single vector", name, class.Class)
		}
		if !isMultiVector && multiShaped {
			return fmt.Errorf("vector %q in collection %q is not configured as multi-vector, but a multi-vector was provided", name, class.Class)
		}
		if isMultiVector {
			if err := validateMultiVectorTokens(class.Class, name, vec); err != nil {
				return err
			}
		}
	}
	return nil
}

// outerVectorLen returns the number of outer elements of a vector payload:
// dimensions for a single vector, tokens for a multi-vector.
func outerVectorLen(vec models.Vector) int {
	switch v := vec.(type) {
	case []float32:
		return len(v)
	case [][]float32:
		return len(v)
	case []interface{}:
		return len(v)
	case [][]interface{}:
		return len(v)
	default:
		return -1
	}
}

// isMultiVectorShaped reports whether the payload is an array of arrays.
// Callers must ensure the outer length is > 0.
func isMultiVectorShaped(vec models.Vector) bool {
	switch v := vec.(type) {
	case [][]float32, [][]interface{}:
		return true
	case []interface{}:
		_, nested := v[0].([]interface{})
		return nested
	default:
		return false
	}
}

// validateMultiVectorTokens rejects multi-vectors containing empty tokens
// (e.g. [[]]), which have no valid encoding.
func validateMultiVectorTokens(className, name string, vec models.Vector) error {
	switch v := vec.(type) {
	case [][]float32:
		for i, token := range v {
			if len(token) == 0 {
				return fmt.Errorf("vector %q in collection %q: multi-vector token %d is empty", name, className, i)
			}
		}
	case [][]interface{}:
		for i, token := range v {
			if len(token) == 0 {
				return fmt.Errorf("vector %q in collection %q: multi-vector token %d is empty", name, className, i)
			}
		}
	case []interface{}:
		for i, token := range v {
			if nested, ok := token.([]interface{}); ok && len(nested) == 0 {
				return fmt.Errorf("vector %q in collection %q: multi-vector token %d is empty", name, className, i)
			}
		}
	}
	return nil
}

// targetVectorizer returns the vectorizer module name configured for a named
// vector, falling back to the class-level vectorizer. "none" means the
// vectors are self-provided.
func targetVectorizer(class *models.Class, name string) string {
	if cfg, ok := class.VectorConfig[name]; ok {
		if vectorizer, ok := cfg.Vectorizer.(map[string]interface{}); ok && len(vectorizer) == 1 {
			for moduleName := range vectorizer {
				return moduleName
			}
		}
	}
	return class.Vectorizer
}
