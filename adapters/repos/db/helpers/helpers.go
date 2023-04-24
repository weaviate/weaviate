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
	"fmt"

	"github.com/weaviate/weaviate/entities/filters"
)

var (
	ObjectsBucket              = []byte("objects")
	ObjectsBucketLSM           = "objects"
	CompressedObjectsBucketLSM = "compressed_objects"
	DimensionsBucketLSM        = "dimensions"
	DocIDBucket                = []byte("doc_ids")
)

// BucketFromPropName creates the byte-representation used as the bucket name
// for a partiular prop in the inverted index
func BucketFromPropName(propName string) []byte {
	return []byte(fmt.Sprintf("property_%s", propName))
}

// MetaCountProp helps create an internally used propName for meta props that
// don't explicitly exist in the user schema, but are required for proper
// indexing, such as the count of arrays.
func MetaCountProp(propName string) string {
	return fmt.Sprintf("%s__meta_count", propName)
}

// BucketFromPropName creates string used as the bucket name
// for a particular prop in the inverted index
func BucketFromPropNameLSM(propName string) string {
	return fmt.Sprintf("property_%s", propName)
}

// HashBucketFromPropName creates string used as the bucket name
// for the status information of a particular prop in the inverted index
func HashBucketFromPropNameLSM(propName string) string {
	return fmt.Sprintf("hash_property_%s", propName)
}

func BucketFromPropNameLengthLSM(propName string) string {
	return BucketFromPropNameLSM(propName + filters.InternalPropertyLength)
}

func HashBucketFromPropNameLengthLSM(propName string) string {
	return HashBucketFromPropNameLSM(propName + filters.InternalPropertyLength)
}

func BucketFromPropNameNullLSM(propName string) string {
	return BucketFromPropNameLSM(propName + filters.InternalNullIndex)
}

func HashBucketFromPropNameNullLSM(propName string) string {
	return HashBucketFromPropNameLSM(propName + filters.InternalNullIndex)
}

func BucketFromPropNameMetaCountLSM(propName string) string {
	return BucketFromPropNameLSM(MetaCountProp(propName))
}

func HashBucketFromPropNameMetaCountLSM(propName string) string {
	return HashBucketFromPropNameLSM(MetaCountProp(propName))
}

func TempBucketFromBucketName(bucketName string) string {
	return bucketName + "_temp"
}

func BucketSearchableFromPropNameLSM(propName string) string {
	return fmt.Sprintf("property_searchable%s", propName)
}
