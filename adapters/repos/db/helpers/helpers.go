//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package helpers

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted/tracker"
	"github.com/weaviate/weaviate/entities/filters"
)

var (
	ObjectsBucket              = []byte("objects")
	ObjectsBucketLSM           = "objects"
	CompressedObjectsBucketLSM = "compressed_objects"
	DimensionsBucketLSM        = "dimensions"
	DocIDBucket                = []byte("doc_ids")
)

func MakePropertyPrefix(property string, propIds *tracker.JsonPropertyIdTracker) []byte {
	propid := propIds.GetIdForProperty(property)
	propid_bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(propid_bytes, propid)
	return propid_bytes
}

func MakePropertyKey(propPrefix []byte, key []byte) []byte {
	if len(propPrefix) == 0 {
		return nil
	}

	t := key[:]
	val := append(propPrefix, t...)

	return val
}

func MatchesPropertyKeyPrefix(propertyName []byte, prefixed_key []byte) bool {
	if len(propertyName) == 0 {
		return false
	}

	return bytes.HasPrefix(prefixed_key, propertyName)
}

func UnMakePropertyKey(propertyName []byte, prefixed_key []byte) []byte {
	if len(propertyName) == 0 {
		return nil
	}

	// duplicate slice
	out := make([]byte, len(prefixed_key)-len(propertyName))
	copy(out, prefixed_key[:len(prefixed_key)-len(propertyName)])

	return out
}

// BucketFrompropertyName creates the byte-representation used as the bucket name
// for a partiular prop in the inverted index
func BucketFrompropertyName(propertyName string) []byte {
	return []byte(fmt.Sprintf("property_%s", propertyName))
}

// MetaCountProp helps create an internally used propertyName for meta props that
// don't explicitly exist in the user schema, but are required for proper
// indexing, such as the count of arrays.
func MetaCountProp(propertyName string) string {
	return fmt.Sprintf("%s__meta_count", propertyName)
}

func PropLength(propertyName string) string {
	return propertyName + filters.InternalPropertyLength
}

func PropNull(propertyName string) string {
	return propertyName + filters.InternalNullIndex
}

// BucketFrompropertyName creates string used as the bucket name
// for a particular prop in the inverted index
func BucketFrompropertyNameLSM(propertyName string) string {
	return fmt.Sprintf("property_%s", propertyName)
}

func BucketFrompropertyNameLengthLSM(propertyName string) string {
	return BucketFrompropertyNameLSM(PropLength(propertyName))
}

func BucketFrompropertyNameNullLSM(propertyName string) string {
	return BucketFrompropertyNameLSM(PropNull(propertyName))
}

func BucketFrompropertyNameMetaCountLSM(propertyName string) string {
	return BucketFrompropertyNameLSM(MetaCountProp(propertyName))
}

func TempBucketFromBucketName(bucketName string) string {
	return bucketName + "_temp"
}

func BucketSearchableFrompropertyNameLSM(propertyName string) string {
	return BucketFrompropertyNameLSM(propertyName + "_searchable")
}
