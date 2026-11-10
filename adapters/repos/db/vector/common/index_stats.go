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

type IndexType string

const (
	IndexTypeHNSW    = "hnsw"
	IndexTypeFlat    = "flat"
	IndexTypeNoop    = "noop"
	IndexTypeDynamic = "dynamic"
	IndexTypeHFresh  = "hfresh"
	// IndexTypePostingEdge is the posting+edges index (provisional name, see
	// adapters/repos/db/vector/postingedge).
	IndexTypePostingEdge = "postingedge"
)

type IndexStats interface {
	IndexType() IndexType
}

func (i IndexType) String() string {
	return string(i)
}

func IsDynamic(indexType IndexType) bool {
	return indexType == IndexTypeDynamic
}
