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

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modelsext"
	"github.com/weaviate/weaviate/entities/storobj"
)

// dropVectorTransformerBuilder returns the objects-bucket TransformerBuilder:
// per remove_target_vectors op it strips the named vectors, re-marshaling only
// when something changed (idempotent no-op otherwise). The disk round-trip must
// match the bucket's own encode/decode (FromBinaryDisk / MarshalBinaryDisk).
func dropVectorTransformerBuilder(className string, skipClassNameOnDisk bool) lsmkv.TransformerBuilder {
	return func(ops []lsmkv.ActiveOp) func([]byte) ([]byte, error) {
		return func(value []byte) ([]byte, error) {
			obj, err := storobj.FromBinaryDisk(value, className)
			if err != nil {
				return nil, fmt.Errorf("decode object for vector drop: %w", err)
			}
			changed := false
			for _, op := range ops {
				if op.Descriptor.Type != lsmkv.OpTypeRemoveTargetVectors {
					continue
				}
				for _, targetVector := range op.Descriptor.Targets {
					if obj.RemoveTargetVector(targetVector) {
						changed = true
					}
				}
			}
			if !changed {
				return value, nil
			}
			return obj.MarshalBinaryDisk(skipClassNameOnDisk)
		}
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
// in cluster/schema/manager.go::apply). Callers gate on named vectors being
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
