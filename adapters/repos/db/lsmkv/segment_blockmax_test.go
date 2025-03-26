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

package lsmkv

import (
	"fmt"
	"testing"

	"github.com/weaviate/weaviate/entities/schema"
)

func TestSerializeAndParseInvertedNodeTest(t *testing.T) {
	t.Skip()
	path := "/Users/amourao/code/weaviate/weaviate/data-weaviate-0/" +
		"msmarco/6Jx2gaSLtsnd/lsm/property_text_searchable/segment-1729794337023372000.db"
	cfg := segmentConfig{
		mmapContents:             false,
		useBloomFilter:           false,
		calcCountNetAdditions:    false,
		overwriteDerived:         true,
		enableChecksumValidation: false,
	}
	seg, err := newSegment(path, nil, nil, nil, cfg)
	if err != nil {
		t.Fatalf("error creating segment: %v", err)
	}

	sbm := NewSegmentBlockMax(seg, []byte("and"), 0, 1, 1, nil, nil, 10, schema.BM25Config{K1: 1.2, B: 0.75})

	sbm.AdvanceAtLeast(100)
	id, score, pair := sbm.Score(1, false)
	sbm.Advance()
	fmt.Println(id, score, pair)
	sbm.AdvanceAtLeast(16000)
	sbm.AdvanceAtLeast(160000000)

	fmt.Println(sbm)
}
