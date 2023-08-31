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

func MakeByteEncodedPropertyPrefix(propertyName string, propertyIds *tracker.JsonPropertyIdTracker) []byte {
	propertyid := propertyIds.GetIdForProperty(propertyName)
	propertyid_bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(propertyid_bytes, propertyid)
	return propertyid_bytes
}

func MakePropertyKey(byteEncodedPropertyId []byte, key []byte) []byte {
	return append(byteEncodedPropertyId, key...)
}

func MatchesPropertyKeyPrefix(byteEncodedPropertyId []byte, prefixed_key []byte) bool {

	return bytes.HasPrefix(prefixed_key, byteEncodedPropertyId)
}

func UnMakePropertyKey(byteEncodedPropertyId []byte, prefixed_key []byte) []byte {

	out := make([]byte, len(prefixed_key)-len(byteEncodedPropertyId))
	copy(out, prefixed_key[:len(prefixed_key)-len(byteEncodedPropertyId)])

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
