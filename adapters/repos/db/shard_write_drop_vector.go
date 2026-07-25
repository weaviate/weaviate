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
	"fmt"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modelsext"
	"github.com/weaviate/weaviate/entities/storobj"
)

// stripDroppedVectors removes dropped-target vectors carried over from a
// previous object version, so a merge cannot re-persist them into a
// post-snapshot segment the cleanup never rewrites (the pending set only covers
// segments present at op registration). The maps are rebuilt, not mutated:
// mergeProps shares the previous object's vector maps with the merged object,
// and the previous version feeds the insert-status comparison.
func stripDroppedVectors(class *models.Class, obj *storobj.Object) {
	var hasDropped bool
	for targetVector := range obj.Vectors {
		if isDroppedVectorIndex(class, targetVector) {
			hasDropped = true
			break
		}
	}
	if hasDropped {
		kept := make(map[string][]float32, len(obj.Vectors))
		for targetVector, vector := range obj.Vectors {
			if !isDroppedVectorIndex(class, targetVector) {
				kept[targetVector] = vector
			}
		}
		obj.Vectors = kept
	}
	hasDropped = false
	for targetVector := range obj.MultiVectors {
		if isDroppedVectorIndex(class, targetVector) {
			hasDropped = true
			break
		}
	}
	if hasDropped {
		kept := make(map[string][][]float32, len(obj.MultiVectors))
		for targetVector, vector := range obj.MultiVectors {
			if !isDroppedVectorIndex(class, targetVector) {
				kept[targetVector] = vector
			}
		}
		obj.MultiVectors = kept
	}
}

// isDroppedVectorIndex reports whether the named vector's index has been
// dropped (VectorConfig entry marked VectorIndexType=="none"). The legacy
// vector (empty targetVector) is not managed through VectorConfig. A nil class
// reports false, leaving the existing GetVectorIndexQueue !ok branch in charge.
func isDroppedVectorIndex(class *models.Class, targetVector string) bool {
	if class == nil || targetVector == "" {
		return false
	}
	cfg, ok := class.VectorConfig[targetVector]
	return ok && modelsext.IsVectorIndexDropped(cfg)
}

// errDroppedVectorIndex mirrors the error GetVectorIndexQueue !ok raises, so a
// dropped vector is indistinguishable from a never-existed one to clients.
func errDroppedVectorIndex(targetVector string) error {
	return fmt.Errorf("vector index not found for %s", targetVector)
}

// rejectDroppedObjectVectors rejects an object carrying a dropped vector. Call
// it before any storage mutation. It also catches the window where the marker
// has applied but the queue teardown has not, during which GetVectorIndexQueue
// still returns a live queue (the marker is applied before the queue is dropped
// within the same schema apply). Callers gate on named vectors being
// present so the schema read is skipped otherwise.
func rejectDroppedObjectVectors(class *models.Class, object *storobj.Object) error {
	for targetVector := range object.Vectors {
		if isDroppedVectorIndex(class, targetVector) {
			return errDroppedVectorIndex(targetVector)
		}
	}
	for targetVector := range object.MultiVectors {
		if isDroppedVectorIndex(class, targetVector) {
			return errDroppedVectorIndex(targetVector)
		}
	}
	return nil
}
