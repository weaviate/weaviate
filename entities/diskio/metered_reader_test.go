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

package diskio

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMeteredReader(t *testing.T) {
	data := make([]byte, 128)

	t.Run("happy path - with callback", func(t *testing.T) {
		var (
			read int64
			took int64
		)

		cb := func(r int64, n int64) {
			read = r
			took = n
		}

		mr := NewMeteredReader(bytes.NewReader(data), cb)

		target := make([]byte, 128)
		n, err := mr.Read(target)

		require.Nil(t, err)
		assert.Equal(t, int64(n), read)
		assert.Greater(t, took, int64(0))
	})

	t.Run("happy path - without callback", func(t *testing.T) {
		mr := NewMeteredReader(bytes.NewReader(data), nil)

		target := make([]byte, 128)
		_, err := mr.Read(target)
		require.Nil(t, err)
	})

	t.Run("with an error", func(t *testing.T) {
		var (
			read int64
			took int64
		)

		cb := func(r int64, n int64) {
			read = r
			took = n
		}

		underlying := bytes.NewReader(data)
		// provoke EOF error by seeking to end of data
		underlying.Seek(128, 0)
		mr := NewMeteredReader(underlying, cb)

		target := make([]byte, 128)
		_, err := mr.Read(target)

		assert.Equal(t, io.EOF, err)

		// callback should not have been called in error cases, so we expect to
		// read initial values
		assert.Equal(t, int64(0), read)
		assert.Equal(t, int64(0), took)
	})
}
