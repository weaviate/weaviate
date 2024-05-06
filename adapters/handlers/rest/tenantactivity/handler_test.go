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

package tenantactivity

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/tenantactivity"
)

func TestHandler_NilHandler(t *testing.T) {
	// this would be the case when the feature is turned off entirely
	var h *Handler
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()

	h.ServeHTTP(w, r)

	res := w.Result()
	defer res.Body.Close()
	assert.Equal(t, http.StatusNotFound, res.StatusCode)
}

func TestHandler_NoSourceSet(t *testing.T) {
	// this would be the case when a request comes in before the DB is fully
	// loaded.
	h := &Handler{}
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()

	h.ServeHTTP(w, r)

	res := w.Result()
	defer res.Body.Close()
	assert.Equal(t, http.StatusServiceUnavailable, res.StatusCode)
	assert.Equal(t, "30", res.Header.Get("retry-after"))
}

func TestHandler_ValidSource(t *testing.T) {
	// normal operation
	now := time.Now()

	h := &Handler{}
	h.SetSource(&dummySource{returnVal: tenantactivity.ByCollection{
		"Col1": tenantactivity.ByTenant{
			"t1": now,
		},
	}})
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()

	h.ServeHTTP(w, r)

	res := w.Result()
	defer res.Body.Close()
	assert.Equal(t, http.StatusOK, res.StatusCode)

	jsonData, err := io.ReadAll(res.Body)
	require.Nil(t, err)

	var act tenantactivity.ByCollection
	err = json.Unmarshal(jsonData, &act)
	require.Nil(t, err)

	assert.Equal(t, now.Format(time.RFC3339Nano), act["Col1"]["t1"].Format(time.RFC3339Nano))
}

type dummySource struct {
	returnVal tenantactivity.ByCollection
}

func (d *dummySource) LocalTenantActivity() tenantactivity.ByCollection {
	return d.returnVal
}
