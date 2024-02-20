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
