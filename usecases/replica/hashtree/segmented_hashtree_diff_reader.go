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

type SegmentedHashTreeDiffReader struct {
	diffReader *CompactHashTreeDiffReader
}

func (ht *SegmentedHashTree) NewDiffReader(diffReader *CompactHashTreeDiffReader) *SegmentedHashTreeDiffReader {
	if diffReader == nil {
		panic("illegal diff reader")
	}

	return &SegmentedHashTreeDiffReader{
		diffReader: diffReader,
	}
}

func (r *SegmentedHashTreeDiffReader) Next() (uint64, uint64, error) {
	leaf0, leaf1, err := r.diffReader.Next()
	if err != nil {
		return 0, 0, err
	}

	// TODO(jeroiraz): map to a range
	return leaf0, leaf1, nil
}
