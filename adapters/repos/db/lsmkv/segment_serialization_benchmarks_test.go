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
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkReplaceNodeKeyIndexAndWriteTo(b *testing.B) {
	// targetBuf := bytes.NewBuffer(make([]byte, 32*1024*1024)) // large enough to avoid growths during running
	targetBuf := bytes.NewBuffer(nil) // large enough to avoid growths during running

	node := segmentReplaceNode{
		tombstone:           true,
		value:               []byte("foo bar"),
		primaryKey:          []byte("foo bar"),
		secondaryIndexCount: 1,
		secondaryKeys:       [][]byte{[]byte("foo bar")},
		offset:              27,
	}

	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := node.KeyIndexAndWriteTo(targetBuf)
		require.Nil(b, err)
	}
}

func BenchmarkCollectionNodeKeyIndexAndWriteTo(b *testing.B) {
	targetBuf := bytes.NewBuffer(make([]byte, 32*1024*1024)) // large enough to avoid growths during running

	node := segmentCollectionNode{
		primaryKey: []byte("foo bar"),
		offset:     27,
		values: []value{
			{
				value:     []byte("my-value"),
				tombstone: true,
			},
			{
				value:     []byte("my-value"),
				tombstone: true,
			},
		},
	}

	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		node.KeyIndexAndWriteTo(targetBuf)
	}
}
