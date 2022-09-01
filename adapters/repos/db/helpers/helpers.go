//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package helpers

import "fmt"

var (
	ObjectsBucket       = []byte("objects")
	ObjectsBucketLSM    = "objects"
	DimensionsBucketLSM = "dimensions"
	DocIDBucket         = []byte("doc_ids")
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

// BucketFromPropName creates the byte-representation used as the bucket name
// for a partiular prop in the inverted index
func BucketFromPropNameLSM(propName string) string {
	return fmt.Sprintf("property_%s", propName)
}

// HashBucketFromPropName creates the byte-representation used as the bucket name
// for the status information of a partiular prop in the inverted index
func HashBucketFromPropNameLSM(propName string) string {
	return fmt.Sprintf("hash_property_%s", propName)
}
