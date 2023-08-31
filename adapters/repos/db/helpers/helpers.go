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

func MakePropertyPrefix(property string, propertyIds *tracker.JsonPropertyIdTracker) []byte {
	propertyid := propertyIds.GetIdForProperty(property)
	propertyid_bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(propertyid_bytes, propertyid)
	return propertyid_bytes
}

func MakePropertyKey(propertyPrefix []byte, key []byte) []byte {

	t := key[:]
	val := append(propertyPrefix, t...)

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

func BucketFromPropertyName(propertyName string) []byte {
	return []byte(fmt.Sprintf("property_%s", propertyName))
}

func MetaCountProperty(propertyName string) string {
	return fmt.Sprintf("%s__meta_count", propertyName)
}

func PropertyLength(propertyName string) string {
	return propertyName + filters.InternalPropertyLength
}

func PropertyNull(propertyName string) string {
	return propertyName + filters.InternalNullIndex
}

func BucketFromPropertyNameLSM(propertyName string) string {
	return fmt.Sprintf("property_%s", propertyName)
}

func BucketFromPropertyNameLengthLSM(propertyName string) string {
	return BucketFromPropertyNameLSM(PropertyLength(propertyName))
}

func BucketFromPropertyNameNullLSM(propertyName string) string {
	return BucketFromPropertyNameLSM(PropertyNull(propertyName))
}

func BucketFromPropertyNameMetaCountLSM(propertyName string) string {
	return BucketFromPropertyNameLSM(MetaCountProperty(propertyName))
}

func TempBucketFromBucketName(bucketName string) string {
	return bucketName + "_temp"
}

func BucketSearchableFromPropertyNameLSM(propertyName string) string {
	return BucketFromPropertyNameLSM(propertyName + "_searchable")
}
