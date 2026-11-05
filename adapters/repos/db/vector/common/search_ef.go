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

package common

import "fmt"

// HonorsSearchEF reports whether a search-time ef can apply to this index.
// Dynamic indexes accept it: the active index is HNSW once the collection
// grows, and an exhaustive flat search is already exact so ef changes nothing.
func HonorsSearchEF(indexType IndexType) bool {
	return indexType == IndexTypeHNSW || indexType == IndexTypeDynamic
}

// UnsupportedSearchEF is the error for a search-time ef on an index that
// cannot apply it.
func UnsupportedSearchEF(targetVector string, indexType IndexType) error {
	if targetVector == "" {
		targetVector = "default"
	}
	return fmt.Errorf("hnsw.ef is only supported by hnsw and dynamic indexes; target vector %q uses %s",
		targetVector, indexType)
}
