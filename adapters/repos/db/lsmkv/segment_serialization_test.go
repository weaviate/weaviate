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
		err := ParseCollectionNodeInto(encoded, &node)
		assert.Nil(t, err)
		assert.Equal(t, expected, node)
	})
}
