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

package extensions

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_StorageHandlers(t *testing.T) {
	ls := newFakeLoaderStorer()
	h := NewRESTHandlers(ls, nil)

	extensionAKey := "my-first-extension"
	extensionAValue := []byte("some-value")

	extensionBKey := "my-other-extension"
	extensionBValue := []byte("some-other-value")

	t.Run("retrieving a non existent concept", func(t *testing.T) {
		r := httptest.NewRequest("GET", "/my-concept", nil)
		w := httptest.NewRecorder()
		h.StorageHandler().ServeHTTP(w, r)

		res := w.Result()
		defer res.Body.Close()
		assert.Equal(t, http.StatusNotFound, res.StatusCode)
	})

	t.Run("storing two extensions", func(t *testing.T) {
		t.Run("extension A", func(t *testing.T) {
			body := bytes.NewReader(extensionAValue)
			r := httptest.NewRequest("PUT", fmt.Sprintf("/%s", extensionAKey), body)
			w := httptest.NewRecorder()
			h.StorageHandler().ServeHTTP(w, r)

			res := w.Result()
			defer res.Body.Close()
			assert.Equal(t, http.StatusOK, res.StatusCode)
		})

		t.Run("extension B", func(t *testing.T) {
			body := bytes.NewReader(extensionBValue)
			r := httptest.NewRequest("PUT", fmt.Sprintf("/%s", extensionBKey), body)
			w := httptest.NewRecorder()
			h.StorageHandler().ServeHTTP(w, r)

			res := w.Result()
			defer res.Body.Close()
			assert.Equal(t, http.StatusOK, res.StatusCode)
		})
	})

	t.Run("when storing fails", func(t *testing.T) {
		ls.storeError = fmt.Errorf("oops")
		body := bytes.NewReader(extensionAValue)
		r := httptest.NewRequest("PUT", "/some-extension", body)

		w := httptest.NewRecorder()
		h.StorageHandler().ServeHTTP(w, r)

		res := w.Result()
		defer res.Body.Close()
		assert.Equal(t, http.StatusInternalServerError, res.StatusCode)
	})

	t.Run("storing with an empty concept", func(t *testing.T) {
		body := bytes.NewReader(extensionAValue)
		r := httptest.NewRequest("PUT", "/", body)

		w := httptest.NewRecorder()
		h.StorageHandler().ServeHTTP(w, r)

		res := w.Result()
		defer res.Body.Close()
		assert.Equal(t, http.StatusNotFound, res.StatusCode)
	})

	t.Run("retrieving two extensions", func(t *testing.T) {
		t.Run("extension A", func(t *testing.T) {
			r := httptest.NewRequest("GET", fmt.Sprintf("/%s", extensionAKey), nil)
			w := httptest.NewRecorder()
			h.StorageHandler().ServeHTTP(w, r)

			res := w.Result()
			defer res.Body.Close()
			assert.Equal(t, http.StatusOK, res.StatusCode)
			assert.Equal(t, extensionAValue, w.Body.Bytes())
		})

		t.Run("extension B", func(t *testing.T) {
			r := httptest.NewRequest("GET", fmt.Sprintf("/%s", extensionBKey), nil)
			w := httptest.NewRecorder()
			h.StorageHandler().ServeHTTP(w, r)

			res := w.Result()
			defer res.Body.Close()
			assert.Equal(t, http.StatusOK, res.StatusCode)
			assert.Equal(t, extensionBValue, w.Body.Bytes())
		})

		t.Run("full dump with trailing slash", func(t *testing.T) {
			r := httptest.NewRequest("GET", "/", nil)
			w := httptest.NewRecorder()
			h.StorageHandler().ServeHTTP(w, r)
			expectedValue := []byte("some-value\nsome-other-value\n")

			res := w.Result()
			defer res.Body.Close()
			assert.Equal(t, http.StatusOK, res.StatusCode)
			assert.Equal(t, expectedValue, w.Body.Bytes())
		})
	})

	t.Run("when loading fails", func(t *testing.T) {
		ls.loadError = fmt.Errorf("oops")
		body := bytes.NewReader(extensionAValue)
		r := httptest.NewRequest("GET", "/some-extension", body)

		w := httptest.NewRecorder()
		h.StorageHandler().ServeHTTP(w, r)

		res := w.Result()
		defer res.Body.Close()
		assert.Equal(t, http.StatusInternalServerError, res.StatusCode)
	})
}

type fakeLoaderStorer struct {
	store      map[string][]byte
	storeError error
	loadError  error
}

func newFakeLoaderStorer() *fakeLoaderStorer {
	return &fakeLoaderStorer{
		store: map[string][]byte{},
	}
}

func (f *fakeLoaderStorer) Store(concept string, value []byte) error {
	if f.storeError == nil {
		f.store[concept] = value
	}
	return f.storeError
}

func (f *fakeLoaderStorer) Load(concept string) ([]byte, error) {
	return f.store[concept], f.loadError
}

func (f *fakeLoaderStorer) LoadAll() ([]byte, error) {
	var keys [][]byte
	for key := range f.store {
		keys = append(keys, []byte(key))
	}

	sort.Slice(keys, func(a, b int) bool {
		return bytes.Compare(keys[a], keys[b]) == -1
	})

	buf := bytes.NewBuffer(nil)
	for _, key := range keys {
		buf.Write(f.store[string(key)])
		buf.Write([]byte("\n"))
	}

	return buf.Bytes(), nil
}
