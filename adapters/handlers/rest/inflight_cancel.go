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
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"time"
)

var errServerShuttingDown = errors.New("server is shutting down")

// inFlightCancel cancels requestsCtx, which every REST request derives from via
// http.Server.BaseContext, so http.Server.Shutdown waits only for handlers that
// ignore it.
type inFlightCancel struct {
	requestsCtx          context.Context
	cancelRequests       context.CancelFunc
	cancelDelay          time.Duration
	unavailableResponses atomic.Int64
}

func newInFlightCancel(cancelDelay time.Duration) *inFlightCancel {
	ctx, cancel := context.WithCancel(context.Background())
	return &inFlightCancel{requestsCtx: ctx, cancelRequests: cancel, cancelDelay: cancelDelay}
}

func (c *inFlightCancel) cancelRequestsAfterDelay() {
	time.AfterFunc(c.cancelDelay, c.cancelRequests)
}

// unavailableAfterCancel answers 503 for a request whose response had not
// started when the cancel fired, and drops the handler's reply. Clients retry
// neither the 200 with per-object errors nor the 500 a cancelled batch answers with.
func (c *inFlightCancel) unavailableAfterCancel(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		uw := &unavailableAfterCancelWriter{ResponseWriter: w, inFlight: c}
		next.ServeHTTP(uw, r)
		uw.decideResponse()
	})
}

type unavailableAfterCancelWriter struct {
	http.ResponseWriter
	inFlight        *inFlightCancel
	responseDecided bool
	replacedWith503 bool
}

// decideResponse runs on the first write or when the handler returns, and
// sends a 503 in place of the handler's response if the cancel already fired.
func (w *unavailableAfterCancelWriter) decideResponse() {
	if w.responseDecided {
		return
	}
	w.responseDecided = true
	if w.inFlight.requestsCtx.Err() == nil {
		return
	}
	w.replacedWith503 = true
	w.inFlight.unavailableResponses.Add(1)
	// A length the handler declared would truncate the 503 body.
	w.ResponseWriter.Header().Del("Content-Length")
	writeOperationalModeErrorResponse(w.ResponseWriter, errServerShuttingDown)
}

func (w *unavailableAfterCancelWriter) WriteHeader(status int) {
	w.decideResponse()
	if !w.replacedWith503 {
		w.ResponseWriter.WriteHeader(status)
	}
}

func (w *unavailableAfterCancelWriter) Write(b []byte) (int, error) {
	w.decideResponse()
	if w.replacedWith503 {
		// go-swagger responders panic on a write error, so report the dropped bytes as written.
		return len(b), nil
	}
	return w.ResponseWriter.Write(b)
}

func (w *unavailableAfterCancelWriter) Flush() {
	w.decideResponse()
	if f, ok := w.ResponseWriter.(http.Flusher); ok && !w.replacedWith503 {
		f.Flush()
	}
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (w *unavailableAfterCancelWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}
