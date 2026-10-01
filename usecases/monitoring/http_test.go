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

package monitoring

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBytesRead(t *testing.T) {
	t.Run("returns the count from an instrumented body", func(t *testing.T) {
		const body = "twelve bytes"
		r := httptest.NewRequest(http.MethodPost, "/v1/batch/objects", strings.NewReader(body))
		r.Body = &countingReadCloser{r: r.Body}

		read, ok := BytesRead(r)
		require.True(t, ok)
		assert.Zero(t, read, "nothing has been read yet")

		_, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		read, ok = BytesRead(r)
		require.True(t, ok)
		assert.Equal(t, int64(len(body)), read)
	})

	t.Run("reports false for a plain body", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPost, "/v1/batch/objects", strings.NewReader("body"))

		_, ok := BytesRead(r)
		assert.False(t, ok, "an uninstrumented body must not read as a zero-byte one")
	})
}
