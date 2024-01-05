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

package clients

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetMeta(t *testing.T) {
	t.Run("when the server is providing meta", func(t *testing.T) {
		server := httptest.NewServer(&testMetaHandler{t: t})
		defer server.Close()
		c := New(server.URL, 0, nullLogger())
		meta, err := c.MetaInfo()

		assert.Nil(t, err)
		assert.NotNil(t, meta)
		metaModel := meta["name"]
		require.NotNil(t, metaModel)
		assert.Equal(t, "Bert", metaModel)
	})
}

type testMetaHandler struct {
	t *testing.T
	// the test handler will report as not ready before the time has passed
	readyTime time.Time
}

func (f *testMetaHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	assert.Equal(f.t, "/meta", r.URL.String())
	assert.Equal(f.t, http.MethodGet, r.Method)

	if time.Since(f.readyTime) < 0 {
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	w.Write([]byte(f.metaInfo()))
}

func (f *testMetaHandler) metaInfo() string {
	return `{
    "description": "<strong>Sbert</strong><br><ul><li>For embeddings",
    "disableGUI": "true",
    "filename": "ggml-all-MiniLM-L6-v2-f16.bin",
    "filesize": "45521167",
    "md5sum": "031bb5d5722c08d13e3e8eaf55c37391",
    "name": "Bert",
    "order": "t",
    "parameters": "1 million",
    "path": "/Users/marcin/.cache/gpt4all/ggml-all-MiniLM-L6-v2-f16.bin",
    "promptTemplate": "### Human: \n{0}\n### Assistant:\n",
    "quant": "f16",
    "ramrequired": "1",
    "requires": "2.4.14",
    "systemPrompt": "",
    "type": "Bert"
}`
}
