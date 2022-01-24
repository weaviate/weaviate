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

package extensions

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_UserFacingHandlers(t *testing.T) {
	proxy := newFakeProxy()
	h := NewRESTHandlers(nil, proxy)

	t.Run("with a method other than POST", func(t *testing.T) {
		r := httptest.NewRequest("GET", "/", nil)
		w := httptest.NewRecorder()
		h.UserFacingHandler().ServeHTTP(w, r)

		res := w.Result()
		defer res.Body.Close()
		assert.Equal(t, http.StatusMethodNotAllowed, res.StatusCode)
	})

	t.Run("with the wrong media type", func(t *testing.T) {
		r := httptest.NewRequest("POST", "/", nil)
		r.Header.Add("content-type", "text/plain")
		w := httptest.NewRecorder()
		h.UserFacingHandler().ServeHTTP(w, r)

		res := w.Result()
		defer res.Body.Close()
		assert.Equal(t, http.StatusUnsupportedMediaType, res.StatusCode)
	})

	t.Run("with the wrong body", func(t *testing.T) {
		body := []byte(`{"concept":7}`)
		r := httptest.NewRequest("POST", "/", bytes.NewReader(body))
		r.Header.Add("content-type", "application/json")
		w := httptest.NewRecorder()
		h.UserFacingHandler().ServeHTTP(w, r)

		res := w.Result()
		defer res.Body.Close()
		assert.Equal(t, http.StatusUnprocessableEntity, res.StatusCode)
	})

	t.Run("with the right body", func(t *testing.T) {
		body := []byte(`{"concept":"foo","definition":"bar","weight":1}`)
		r := httptest.NewRequest("POST", "/", bytes.NewReader(body))
		r.Header.Add("content-type", "application/json")
		w := httptest.NewRecorder()
		h.UserFacingHandler().ServeHTTP(w, r)

		res := w.Result()
		defer res.Body.Close()

		readBody, err := ioutil.ReadAll(res.Body)
		require.Nil(t, err)
		assert.Equal(t, http.StatusOK, res.StatusCode)
		assert.Equal(t, body, readBody)
	})

	t.Run("with a proxy error", func(t *testing.T) {
		proxy.err = errors.Errorf("invalid input")
		body := []byte(`{"concept":"foo","definition":"bar","weight":1}`)
		r := httptest.NewRequest("POST", "/", bytes.NewReader(body))
		r.Header.Add("content-type", "application/json")
		w := httptest.NewRecorder()
		h.UserFacingHandler().ServeHTTP(w, r)

		res := w.Result()
		defer res.Body.Close()
		assert.Equal(t, http.StatusBadRequest, res.StatusCode)
	})
}

type fakeProxy struct {
	err error
}

func (f *fakeProxy) AddExtension(ctx context.Context,
	ext *models.C11yExtension) error {
	return f.err
}

func newFakeProxy() *fakeProxy {
	return &fakeProxy{}
}
