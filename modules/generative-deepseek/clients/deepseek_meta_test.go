//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package clients

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeepSeekGetMeta(t *testing.T) {
	t.Run("when the server is providing meta", func(t *testing.T) {
		handler := &deepseekMetaHandler{t: t}
		server := httptest.NewServer(handler)
		defer server.Close()

		c := New("", 0, nullLogger())
		meta, err := c.MetaInfo()

		assert.Nil(t, err)
		assert.NotNil(t, meta)
		assert.Equal(t, "Generative Search - DeepSeek", meta["name"])
		assert.Equal(t, "https://api-docs.deepseek.com/", meta["documentationHref"])
	})
}

type deepseekMetaHandler struct {
	t *testing.T
}

func (f *deepseekMetaHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Simple check to ensure not ServiceUnavailable
	w.Write([]byte(`{}`))
}
