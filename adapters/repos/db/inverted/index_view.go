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

package inverted

import (
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
)

// IndexView is a shard's property indexes and their settings, taken once per
// query so a reindex switch never pairs a bucket with another tokenization.
type IndexView interface {
	lsmkv.BucketLookup
	Tokenization(propName, schemaTokenization string) string
	// RangeableReady reports whether range filters on prop may read its
	// rangeable index. Otherwise they walk the filterable one.
	RangeableReady(prop *models.Property) bool
	FallbackToSearchable() bool
}
