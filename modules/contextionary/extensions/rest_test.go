package extensions

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHandlers(t *testing.T) {
	ls := newFakeLoaderStorer()
	h := NewRESTHandlers(ls)

	extensionAKey := "my-first-extension"
	extensionAValue := []byte("some-value")

	extensionBKey := "my-other-extension"
	extensionBValue := []byte("some-other-value")

	t.Run("retrieving a non existent concept", func(t *testing.T) {
		r := httptest.NewRequest("GET", "/my-concept", nil)
		w := httptest.NewRecorder()
		h.Handler().ServeHTTP(w, r)

		res := w.Result()
		defer res.Body.Close()
		assert.Equal(t, http.StatusNotFound, res.StatusCode)
	})

	t.Run("storing two extensions", func(t *testing.T) {
		t.Run("extension A", func(t *testing.T) {
			body := bytes.NewReader(extensionAValue)
			r := httptest.NewRequest("PUT", fmt.Sprintf("/%s", extensionAKey), body)
			w := httptest.NewRecorder()
			h.Handler().ServeHTTP(w, r)

			res := w.Result()
			defer res.Body.Close()
			assert.Equal(t, http.StatusOK, res.StatusCode)
		})

		t.Run("extension B", func(t *testing.T) {
			body := bytes.NewReader(extensionBValue)
			r := httptest.NewRequest("PUT", fmt.Sprintf("/%s", extensionBKey), body)
			w := httptest.NewRecorder()
			h.Handler().ServeHTTP(w, r)

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
		h.Handler().ServeHTTP(w, r)

		res := w.Result()
		defer res.Body.Close()
		assert.Equal(t, http.StatusInternalServerError, res.StatusCode)
	})

	t.Run("storing with an empty concept", func(t *testing.T) {
		body := bytes.NewReader(extensionAValue)
		r := httptest.NewRequest("PUT", "/", body)

		w := httptest.NewRecorder()
		h.Handler().ServeHTTP(w, r)

		res := w.Result()
		defer res.Body.Close()
		assert.Equal(t, http.StatusNotFound, res.StatusCode)
	})

	t.Run("retrieving two extensions", func(t *testing.T) {
		t.Run("extension A", func(t *testing.T) {
			r := httptest.NewRequest("GET", fmt.Sprintf("/%s", extensionAKey), nil)
			w := httptest.NewRecorder()
			h.Handler().ServeHTTP(w, r)

			res := w.Result()
			defer res.Body.Close()
			assert.Equal(t, http.StatusOK, res.StatusCode)
			assert.Equal(t, extensionAValue, w.Body.Bytes())
		})

		t.Run("extension B", func(t *testing.T) {
			r := httptest.NewRequest("GET", fmt.Sprintf("/%s", extensionBKey), nil)
			w := httptest.NewRecorder()
			h.Handler().ServeHTTP(w, r)

			res := w.Result()
			defer res.Body.Close()
			assert.Equal(t, http.StatusOK, res.StatusCode)
			assert.Equal(t, extensionBValue, w.Body.Bytes())
		})
	})

	t.Run("load with an empty concept name", func(t *testing.T) {
		r := httptest.NewRequest("GET", "/", nil)

		w := httptest.NewRecorder()
		h.Handler().ServeHTTP(w, r)

		res := w.Result()
		defer res.Body.Close()
		assert.Equal(t, http.StatusNotFound, res.StatusCode)
	})

	t.Run("when loading fails", func(t *testing.T) {
		ls.loadError = fmt.Errorf("oops")
		body := bytes.NewReader(extensionAValue)
		r := httptest.NewRequest("GET", "/some-extension", body)

		w := httptest.NewRecorder()
		h.Handler().ServeHTTP(w, r)

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
	f.store[concept] = value
	return f.storeError
}

func (f *fakeLoaderStorer) Load(concept string) ([]byte, error) {
	return f.store[concept], f.loadError
}
