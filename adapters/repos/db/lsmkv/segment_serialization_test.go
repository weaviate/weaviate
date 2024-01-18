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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_SerializeAndParseCollectionNode(t *testing.T) {
	before := segmentCollectionNode{
		primaryKey: []byte("this-is-my-primary-key"),
		values: []value{{
			value: []byte("the-first-value"),
		}, {
			value:     []byte("the-second-value-with-a-tombstone"),
			tombstone: true,
		}},
	}

	buf := &bytes.Buffer{}
	_, err := before.KeyIndexAndWriteTo(buf)
	require.Nil(t, err)
	encoded := buf.Bytes()

	expected := segmentCollectionNode{
		primaryKey: []byte("this-is-my-primary-key"),
		values: []value{{
			value: []byte("the-first-value"),
		}, {
			value:     []byte("the-second-value-with-a-tombstone"),
			tombstone: true,
		}},
		offset: buf.Len(),
	}

	t.Run("parse using the _regular_ way", func(t *testing.T) {
		after, err := ParseCollectionNode(bytes.NewReader(encoded))
		assert.Nil(t, err)
		assert.Equal(t, expected, after)
	})

	t.Run("parse using the reusable way", func(t *testing.T) {
		var node segmentCollectionNode
		err := ParseCollectionNodeInto(bytes.NewReader(encoded), &node)
		assert.Nil(t, err)
		assert.Equal(t, expected, node)
	})
}
