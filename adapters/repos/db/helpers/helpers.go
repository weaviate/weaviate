//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
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
	VectorsCompressedBucketLSM = "vectors_compressed"
	VectorsBucketLSM           = "vectors"
	DimensionsBucketLSM        = "dimensions"
	mergemode = "prefix"
)

func makeByteEncodedPropertyPostfix(propertyName string, propertyIds *tracker.JsonPropertyIdTracker) []byte {
	propertyid := propertyIds.GetIdForProperty(propertyName)
	propertyid_bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(propertyid_bytes, propertyid)
	return propertyid_bytes
}

func makeByteEncodedPropertyPrefix(propertyName string, propertyIds *tracker.JsonPropertyIdTracker) []byte {
	propertyid := propertyIds.GetIdForProperty(propertyName)
	propertyid_bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(propertyid_bytes, propertyid)
	return propertyid_bytes
}

func MakeByteEncodedProperty(propertyName string, propertyIds *tracker.JsonPropertyIdTracker) []byte {
	if mergemode == "prefix" {
		return makeByteEncodedPropertyPrefix(propertyName, propertyIds)
	} else {
		return makeByteEncodedPropertyPostfix(propertyName, propertyIds)
	}
}

// BucketFromPropName creates the byte-representation used as the bucket name
// for a particular prop in the inverted index
func BucketFromPropName(propName string) []byte {
	return []byte(fmt.Sprintf("property_%s", propName))
}

func makePropertyKeyPostfix(byteEncodedPropertyId []byte, key []byte) []byte {
	b := make([]byte, len(byteEncodedPropertyId))
	copy(b, byteEncodedPropertyId)

	k := make([]byte, len(key))
	copy(k, key)

	jointKey := append(k, b...)
	return jointKey
}

func makePropertyKeyPrefix(byteEncodedPropertyId []byte, key []byte) []byte {
	b := make([]byte, len(byteEncodedPropertyId))
	copy(b, byteEncodedPropertyId)

	k := make([]byte, len(key))
	copy(k, key)

	jointKey := append(b, k...)
	return jointKey
}

func MakePropertyKey(byteEncodedPropertyId []byte, key []byte) []byte {
	if mergemode == "prefix" {
		return makePropertyKeyPrefix(byteEncodedPropertyId, key)
	} else {
		return makePropertyKeyPostfix(byteEncodedPropertyId, key)
	}
}

// MetaCountProp helps create an internally used propName for meta props that
// don't explicitly exist in the user schema, but are required for proper
// indexing, such as the count of arrays.
func MetaCountProp(propName string) string {
	return fmt.Sprintf("%s__meta_count", propName)
}

func matchesPropertyKeyPostfix(byteEncodedPropertyId []byte, prefixed_key []byte) bool {
	// Allows accessing every key
	if len(byteEncodedPropertyId) == 0 {
		return true
	}

	return bytes.HasSuffix(prefixed_key, byteEncodedPropertyId)
}

func matchesPropertyKeyPrefix(byteEncodedPropertyId []byte, postfixed_key []byte) bool {
	// Allows accessing every key
	if len(byteEncodedPropertyId) == 0 {
		return true
	}

	return bytes.HasPrefix(postfixed_key, byteEncodedPropertyId)
}

func MatchesPropertyKey(byteEncodedPropertyId []byte, postfixed_key []byte) bool {
	if mergemode == "prefix" {
		return matchesPropertyKeyPrefix(byteEncodedPropertyId, postfixed_key)
	} else {
		return matchesPropertyKeyPostfix(byteEncodedPropertyId, postfixed_key)
	}
}



func unMakePropertyKeyPostfix(byteEncodedPropertyId []byte, postfixed_key []byte) []byte {
	// For postfix propid
	out := make([]byte, len(postfixed_key)-len(byteEncodedPropertyId))
	copy(out, postfixed_key[:len(postfixed_key)-len(byteEncodedPropertyId)])

	return out
}

func unMakePropertyKeyPrefix(byteEncodedPropertyId []byte, prefixed_key []byte) []byte {
	// For prefix propid
	out := make([]byte, len(prefixed_key)-len(byteEncodedPropertyId))
	copy(out, prefixed_key[len(byteEncodedPropertyId):])

	return out
}

func UnMakePropertyKey(byteEncodedPropertyId []byte, key []byte) []byte {
	if mergemode == "prefix" {
		return unMakePropertyKeyPrefix(byteEncodedPropertyId, key)
	} else {
		return unMakePropertyKeyPostfix(byteEncodedPropertyId, key)
	}
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
