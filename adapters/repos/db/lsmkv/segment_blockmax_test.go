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
	seg, err := newSegment("/Users/amourao/code/weaviate/weaviate/data-weaviate-0/fiqa/CDg41dTD5QYH/lsm/property_text_searchable/segment-1728597278780046000.db", nil,
		nil, nil, false, false, false, true)
	if err != nil {
		t.Fatalf("error creating segment: %v", err)
	}

	sbm := NewSegmentBlockMax(seg, []byte("test"), 0, 1)

	sbm.AdvanceAtLeast(100)
	id, score, pair := sbm.ScoreAndAdvance(1, schema.BM25Config{K1: 1.2, B: 0.75})
	fmt.Println(id, score, pair)
	sbm.AdvanceAtLeast(16000)
	sbm.AdvanceAtLeast(160000000)

	fmt.Println(sbm)
}
