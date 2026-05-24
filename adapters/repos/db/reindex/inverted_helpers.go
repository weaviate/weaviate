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

package reindex

import (
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/entities/filters"
	entinverted "github.com/weaviate/weaviate/entities/inverted"
)

// Property classification helpers moved here from
// shard_write_inverted_lsm.go; the reindex strategies need the same
// rules the regular write path uses.

func isMetaCountProperty(property inverted.Property) bool {
	return len(property.Name) > 12 && property.Name[len(property.Name)-12:] == "__meta_count"
}

func isInternalProperty(property inverted.Property) bool {
	return property.Name[0] == '_'
}

// Bucket-key encoders for the special internal indices the reindex
// strategies populate alongside the value indices.

func bucketKeyPropertyLength(length int) ([]byte, error) {
	return entinverted.LexicographicallySortableInt64(int64(length))
}

func bucketKeyPropertyNull(isNull bool) ([]byte, error) {
	if isNull {
		return []byte{uint8(filters.InternalNullState)}, nil
	}
	return []byte{uint8(filters.InternalNotNullState)}, nil
}
