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

package transformers

import (
	"fmt"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/editops"
	"github.com/weaviate/weaviate/entities/storobj"
)

// dropVectorTransformer is the OpTransformerFactory for OpTypeRemoveTargetVectors:
// given the live remove-target-vector ops it strips the named vectors from each
// stored object, re-marshaling only when something changed (idempotent no-op
// otherwise). Every op handed in is already of this type — the registry dispatches
// by type — so no type check is needed here. className is needed to decode the
// object (FromBinaryDisk stamps the bucket's class name, ignoring the on-disk one).
// Re-marshal always writes the class name (MarshalBinaryDisk(false)): readers stamp
// the bucket class name regardless, so the on-disk class-name field is not load-bearing.
func dropVectorTransformer(className string, ops []editops.ActiveOp) func([]byte) ([]byte, error) {
	return func(value []byte) ([]byte, error) {
		obj, err := storobj.FromBinaryDisk(value, className)
		if err != nil {
			return nil, fmt.Errorf("decode object for vector drop: %w", err)
		}
		changed := false
		for _, op := range ops {
			for _, targetVector := range op.Descriptor.Targets {
				if obj.RemoveTargetVector(targetVector) {
					changed = true
				}
			}
		}
		if !changed {
			return value, nil
		}
		return obj.MarshalBinaryDisk(false)
	}
}
