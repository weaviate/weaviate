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

package hashtree

type CompactHashTreeDiffReader struct {
	diffReader *HashTreeDiffReader
}

func (ht *CompactHashTree) NewDiffReader(diffReader *HashTreeDiffReader) *CompactHashTreeDiffReader {
	if diffReader == nil {
		panic("illegal diff reader")
	}

	return &CompactHashTreeDiffReader{
		diffReader: diffReader,
	}
}

func (r *CompactHashTreeDiffReader) Next() (uint64, uint64, error) {
	leaf, err := r.diffReader.Next()
	if err != nil {
		return 0, 0, err
	}

	// TODO(jeroiraz): map to a range
	return uint64(leaf), uint64(leaf), nil
}
