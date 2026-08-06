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
	"slices"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
)

// columnarContainsConfigured reports whether this shard's collection asked for a
// resident columnar ContainsAny index on propName.
//
// The configuration names collections and, within them, properties: a collection
// listed with properties enables exactly those, and a collection listed without
// any enables all of them. Nothing is enabled by default — the index trades
// memory for ContainsAny speed and assumes near-unique values, so it is opted
// into per property rather than inferred from the schema.
func (s *Shard) columnarContainsConfigured(propName string) bool {
	configured, ok := s.index.Config.ColumnarContainsIndexes[s.index.Config.ClassName.String()]
	if !ok {
		return false
	}
	if len(configured) == 0 {
		return true // collection named without properties: all of them
	}
	return slices.Contains(configured, propName)
}

// detachColumnarContainsIndex drops the columnar ContainsAny index from
// propName's filterable bucket, if it carries one. Called when the property's
// tokenization is changing: the index was built by reading the bucket's keys,
// and a retokenization rewrites what those keys are, which it has no way to
// notice on its own.
func (s *Shard) detachColumnarContainsIndex(propName string) {
	if propName == "" || s.store == nil {
		return
	}
	if bkt := s.store.Bucket(helpers.BucketFromPropNameLSM(propName)); bkt != nil {
		bkt.DetachColumnarContainsIndex()
	}
}
