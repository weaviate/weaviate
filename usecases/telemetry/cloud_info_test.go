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

package telemetry

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

// statusServer returns the URL of a throwaway server that answers every request
// with the given HTTP status, so cloud detection can be exercised offline.
func statusServer(t *testing.T, status int) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestAWSCloudInfoIsDetected(t *testing.T) {
	// AWS metadata answers 200, or 401 when an IMDSv2 token is required; both
	// mean "running on AWS". Anything else means not detected.
	assert.True(t, newAWSCloudInfo(statusServer(t, http.StatusOK)).isDetected())
	assert.True(t, newAWSCloudInfo(statusServer(t, http.StatusUnauthorized)).isDetected())
	assert.False(t, newAWSCloudInfo(statusServer(t, http.StatusInternalServerError)).isDetected())
}

func TestGCPCloudInfoIsDetected(t *testing.T) {
	assert.True(t, newGCPCloudInfo(statusServer(t, http.StatusOK)).isDetected())
	assert.False(t, newGCPCloudInfo(statusServer(t, http.StatusInternalServerError)).isDetected())
}

func TestAzureCloudInfoIsDetected(t *testing.T) {
	assert.True(t, newAzureCloudInfo(statusServer(t, http.StatusOK), "2021-02-01").isDetected())
	assert.False(t, newAzureCloudInfo(statusServer(t, http.StatusInternalServerError), "2021-02-01").isDetected())
}
