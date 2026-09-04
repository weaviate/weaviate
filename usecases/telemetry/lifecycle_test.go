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
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewAppliesDefaults pins that an empty consumer URL and a zero push
// interval fall back to the package defaults (the `== ""` / `== 0` guards in New).
func TestNewAppliesDefaults(t *testing.T) {
	tel, _, _ := newTestTelemeter() // built with New(..., "", 0, ...)
	assert.Equal(t, DefaultTelemetryConsumerURL, tel.consumer)
	assert.Equal(t, DefaultTelemetryPushInterval, tel.pushInterval)
}

// TestStopWithNilClientTracker pins the `if tel.clientTracker != nil` guard in
// Stop's deferred cleanup: a nil tracker must be skipped, not dereferenced.
func TestStopWithNilClientTracker(t *testing.T) {
	logger, _ := test.NewNullLogger()
	tel := &Telemeter{
		logger:        logger,
		failedToStart: true, // short-circuit before the network push
		shutdown:      make(chan struct{}),
		// clientTracker is intentionally nil
	}
	require.NoError(t, tel.Stop(context.Background()))
}

// TestClientTrackingMiddlewareNilTracker pins the `if tracker == nil` guard:
// a nil tracker must yield a pass-through middleware. The request carries a
// recognized client header so that a `!= nil` mutant (which builds the real
// middleware around the nil tracker) would dereference it in Track and panic —
// whereas the correct no-op path just forwards the request.
func TestClientTrackingMiddlewareNilTracker(t *testing.T) {
	called := false
	final := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true })

	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.Header.Set("X-Weaviate-Client", "weaviate-client-python/4.5.0")

	h := ClientTrackingMiddleware(nil)(final)
	h.ServeHTTP(httptest.NewRecorder(), req)

	assert.True(t, called, "nil tracker must produce a pass-through middleware")
}
