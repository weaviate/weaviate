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

package hashtree

import (
	"testing"

	"github.com/spaolacci/murmur3"
	"github.com/stretchr/testify/require"
)

func TestDigestMarshallingUnmarshalling(t *testing.T) {
	var d1 Digest

	hash := murmur3.New128()

	_, err := hash.Write([]byte("hashtree"))
	require.NoError(t, err)

	d1[0], d1[1] = hash.Sum128()

	var d2 Digest

	b, err := d1.MarshalJSON()
	require.NoError(t, err)

	err = d2.UnmarshalJSON(b)
	require.NoError(t, err)

	require.Equal(t, d1, d2)
}

func TestDigestsBinaryCodec(t *testing.T) {
	digests := []Digest{{1, 2}, {0, 0}, {^uint64(0), ^uint64(0)}, {1 << 63, 42}}

	encoded := DigestsToBinary(digests)
	require.Len(t, encoded, len(digests)*DigestLength)

	for i := range digests {
		single, err := digests[i].MarshalBinary()
		require.NoError(t, err)
		require.Equal(t, single, encoded[i*DigestLength:(i+1)*DigestLength])
	}

	decoded, err := DigestsFromBinary(encoded)
	require.NoError(t, err)
	require.Equal(t, digests, decoded)

	decoded, err = DigestsFromBinary(nil)
	require.NoError(t, err)
	require.Empty(t, decoded)

	require.Empty(t, DigestsToBinary(nil))

	for _, n := range []int{1, 15, 17, 31, DigestLength + 1} {
		_, err := DigestsFromBinary(make([]byte, n))
		require.Error(t, err, "length %d", n)
	}

	_, err = DigestsFromBinary([]byte("[]"))
	require.Error(t, err)
}
