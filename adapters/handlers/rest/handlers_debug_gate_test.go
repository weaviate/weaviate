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

package rest

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
)

func TestDebugEndpointsGate(t *testing.T) {
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	t.Run("nil flag returns 404", func(t *testing.T) {
		h := makeDebugEndpointsGate(nil)(ok)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/debug/config", nil))
		assert.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("flag false returns 404", func(t *testing.T) {
		flag := configRuntime.NewDynamicValue(false)
		h := makeDebugEndpointsGate(flag)(ok)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/debug/config", nil))
		assert.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("flag true passes through", func(t *testing.T) {
		flag := configRuntime.NewDynamicValue(true)
		h := makeDebugEndpointsGate(flag)(ok)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/debug/config", nil))
		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("runtime flip from true to false starts returning 404", func(t *testing.T) {
		flag := configRuntime.NewDynamicValue(true)
		h := makeDebugEndpointsGate(flag)(ok)

		w1 := httptest.NewRecorder()
		h.ServeHTTP(w1, httptest.NewRequest(http.MethodGet, "/debug/config", nil))
		assert.Equal(t, http.StatusOK, w1.Code, "before flip")

		flag.SetValue(false)

		w2 := httptest.NewRecorder()
		h.ServeHTTP(w2, httptest.NewRequest(http.MethodGet, "/debug/config", nil))
		assert.Equal(t, http.StatusNotFound, w2.Code, "after flip to false")

		flag.SetValue(true)

		w3 := httptest.NewRecorder()
		h.ServeHTTP(w3, httptest.NewRequest(http.MethodGet, "/debug/config", nil))
		assert.Equal(t, http.StatusOK, w3.Code, "after flip back to true")
	})
}
