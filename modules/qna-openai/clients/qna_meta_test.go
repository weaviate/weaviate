//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package clients

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGetMeta(t *testing.T) {
	t.Run("when the server is providing meta", func(t *testing.T) {
		server := httptest.NewServer(&testMetaHandler{t: t})
		defer server.Close()
		c := New(server.URL, nullLogger())
		meta, err := c.MetaInfo()

		assert.Nil(t, err)
		assert.NotNil(t, meta)
		metaModel := meta["name"]
		assert.True(t, metaModel != nil)
		documentationHref := meta["documentationHref"]
		assert.True(t, documentationHref != nil)
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
  "hostname": "http://127.0.0.1:8080",
  "modules": {
    "qna-openai": {
      "documentationHref": "https://beta.openai.com/docs/api-reference/completions",
      "name": "OpenAI Question & Answering Module"
    }
  },
  "version": "1.16.0"
}`
}
