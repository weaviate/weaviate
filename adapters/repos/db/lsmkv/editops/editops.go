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

// Package editops holds the neutral type vocabulary for segment edit operations:
// the op descriptors persisted in the lsmkv sidecar and the transformer-factory
// signature. It deliberately depends on neither lsmkv nor storobj so that both
// the storage layer (lsmkv, which records and applies ops) and the transformer
// implementations (package transformers, which need storobj) can import it
// without an import cycle.
package editops

// OpType discriminates an edit operation. lsmkv records it opaquely; the
// transformers registry maps it to the function that performs the rewrite.
type OpType string

// OpTypeRemoveTargetVectors strips dropped named vectors from stored objects.
const OpTypeRemoveTargetVectors OpType = "remove_target_vectors"

// OpDescriptor describes a single edit operation. It is opaque to the rewrite
// machinery beyond Type, which selects the transformer to apply.
type OpDescriptor struct {
	// Type discriminates the operation; OpTypeRemoveTargetVectors today.
	Type OpType `json:"type"`
	// Targets are the operands, e.g. the named vectors to strip.
	Targets []string `json:"targets"`
	// CreatedAt is a monotonic timestamp (caller-supplied) that orders
	// transformer application when multiple ops are active.
	CreatedAt int64 `json:"createdAt"`
}

// ActiveOp pairs an operation's ID with its descriptor.
type ActiveOp struct {
	ID         string
	Descriptor OpDescriptor
}

// OpTransformerFactory builds the value transformer for all live ops of a single
// OpType (the registry key guarantees every op handed to it shares that type).
// className is the bucket's canonical class name, which the transformer needs to
// decode the object; lsmkv supplies it at build time so the factory itself can live
// in a global registry without capturing bucket state. The returned function
// operates on raw value bytes.
type OpTransformerFactory func(className string, ops []ActiveOp) func(value []byte) ([]byte, error)
