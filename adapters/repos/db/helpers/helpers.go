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

func MakeByteEncodedPropertyPostfix(propertyName string, propertyIds *tracker.JsonPropertyIdTracker) []byte {
	propertyid := propertyIds.GetIdForProperty(propertyName)
	propertyid_bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(propertyid_bytes, propertyid)
	return propertyid_bytes
}

// BucketFromPropName creates the byte-representation used as the bucket name
// for a particular prop in the inverted index
func BucketFromPropName(propName string) []byte {
	return []byte(fmt.Sprintf("property_%s", propName))
}

func MakePropertyKey(byteEncodedPropertyId []byte, key []byte) []byte {
	b := make([]byte, len(byteEncodedPropertyId))
	copy(b, byteEncodedPropertyId)

	k := make([]byte, len(key))
	copy(k, key)

	return append(k, b...)
}

func MatchesPropertyKeyPostfix(byteEncodedPropertyId []byte, prefixed_key []byte) bool {
	// Allows accessing every key
	if len(byteEncodedPropertyId) == 0 {
		return true
	}

	return bytes.HasSuffix(prefixed_key, byteEncodedPropertyId)
}

func UnMakePropertyKey(byteEncodedPropertyId []byte, postfixed_key []byte) []byte {
	/*
		//For prefix
		out := make([]byte, len(prefixed_key)-len(byteEncodedPropertyId))
		copy(out, prefixed_key[len(byteEncodedPropertyId):])
	*/
	//For postfix propid
	out := make([]byte, len(postfixed_key)-len(byteEncodedPropertyId))
	copy(out, postfixed_key[:len(postfixed_key)-len(byteEncodedPropertyId)])

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
