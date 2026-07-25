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

// Package transformers is the single source of truth for the segment edit-op
// value transformers. Each supported OpType maps to one factory here; lsmkv reads
// the ops persisted in a segment's edit-ops sidecar and instantiates the matching
// factory via Lookup. Adding a new edit operation means adding one entry to the
// registry below — no bucket wiring changes.
//
// It imports storobj (the transformers operate on the on-disk object format) and
// the neutral editops types, but never lsmkv, so lsmkv can import it for Lookup
// without an import cycle.
package transformers

import "github.com/weaviate/weaviate/adapters/repos/db/lsmkv/editops"

// registry maps every supported op type to its transformer factory.
var registry = map[editops.OpType]editops.OpTransformerFactory{
	editops.OpTypeRemoveTargetVectors: dropVectorTransformer,
}

// Lookup returns the factory registered for opType, and whether one exists. lsmkv
// calls this per op type present in a sidecar; an unknown type yields ok=false and
// is skipped (a transformation the running binary doesn't support).
func Lookup(opType editops.OpType) (editops.OpTransformerFactory, bool) {
	f, ok := registry[opType]
	return f, ok
}
