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

/*
func Test_SerializeAndParseInvertedNode(t *testing.T) {
	tfs := []float32{2.0, 3.0, 4.0}
	lens := []uint32{1, 2, 3}

	values := make([]value, len(tfs))

	for i := range tfs {
		buf := make([]byte, invPayloadLen)
		binary.LittleEndian.PutUint64(buf[0:8], uint64(i))
		binary.LittleEndian.PutUint32(buf[8:12], math.Float32bits(tfs[i]))
		binary.LittleEndian.PutUint32(buf[12:16], lens[i])

		values[i] = value{value: buf}
	}

	before := segmentInvertedNode{
		primaryKey: []byte("this-is-my-primary-key"),
		values:     values,
	}

	buf := &bytes.Buffer{}
	_, err := before.KeyIndexAndWriteTo(buf)
	require.Nil(t, err)
	encoded := buf.Bytes()

	expected := segmentCollectionNode{
		primaryKey: []byte("this-is-my-primary-key"),
		values:     values,
		offset:     buf.Len(),
	}

	t.Run("parse using the _regular_ way", func(t *testing.T) {
		after, err := ParseInvertedNode(bytes.NewReader(encoded))
		assert.Nil(t, err)
		assert.Equal(t, expected, after)
	})

	t.Run("parse using the reusable way", func(t *testing.T) {
		var node segmentCollectionNode
		err := ParseInvertedNodeInto(bytes.NewReader(encoded), &node)
		assert.Nil(t, err)
		assert.Equal(t, expected, node)
	})
}
*/
