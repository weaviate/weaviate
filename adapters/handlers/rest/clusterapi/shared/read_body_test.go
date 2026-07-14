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

package shared

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadBody(t *testing.T) {
	payload := []byte("internode payload bytes")

	t.Run("known content length", func(t *testing.T) {
		out, err := ReadBody(bytes.NewReader(payload), int64(len(payload)))
		require.NoError(t, err)
		assert.Equal(t, payload, out)
		// exact-size allocation: no spare capacity from growth
		assert.Equal(t, len(payload), cap(out))
	})

	t.Run("unknown content length falls back to ReadAll", func(t *testing.T) {
		for _, cl := range []int64{-1, 0} {
			out, err := ReadBody(bytes.NewReader(payload), cl)
			require.NoError(t, err)
			assert.Equal(t, payload, out)
		}
	})

	t.Run("empty body with zero content length", func(t *testing.T) {
		out, err := ReadBody(bytes.NewReader(nil), 0)
		require.NoError(t, err)
		assert.Empty(t, out)
	})

	t.Run("body shorter than declared length", func(t *testing.T) {
		_, err := ReadBody(bytes.NewReader(payload), int64(len(payload))+10)
		require.Error(t, err)
		assert.True(t, errors.Is(err, io.ErrUnexpectedEOF), "got %v", err)
	})

	t.Run("declared length shorter than body reads only declared bytes", func(t *testing.T) {
		// net/http bounds body readers at Content-Length, so this cannot
		// happen with real HTTP bodies; the helper still behaves sanely.
		out, err := ReadBody(bytes.NewReader(payload), 4)
		require.NoError(t, err)
		assert.Equal(t, payload[:4], out)
	})
}
