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
	"strconv"
	"strings"
	"sync/atomic"

	"google.golang.org/grpc/codes"
)

var errServerShuttingDown = errors.New("server is shutting down")

// inFlightCancel refuses requests through its middleware once requestsCtx is
// cancelled, and replaces responses not yet started.
type inFlightCancel struct {
	requestsCtx          context.Context
	unavailableResponses atomic.Int64
	shutdownStarted      atomic.Bool
}

func newInFlightCancel(requestsCtx context.Context) *inFlightCancel {
	return &inFlightCancel{requestsCtx: requestsCtx}
}

// startShutdown makes readiness answer 503.
func (c *inFlightCancel) startShutdown() {
	c.shutdownStarted.Store(true)
}

// unavailableAfterCancel refuses new requests once the cancel fired, and replaces
// responses not yet started. Clients retry neither the 200 with per-object
// errors nor the 500 a cancelled batch answers with.
func (c *inFlightCancel) unavailableAfterCancel(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestContentType := r.Header.Get("Content-Type")
		if c.requestsCtx.Err() != nil {
			c.unavailableResponses.Add(1)
			writeShuttingDown(w, requestContentType)
			return
		}
		uw := &unavailableAfterCancelWriter{ResponseWriter: w, inFlight: c, requestContentType: requestContentType}
		defer uw.decideResponse()
		next.ServeHTTP(uw, r)
	})
}

// writeShuttingDown answers grpc-web with a trailers-only Grpc-Status 14, which
// grpc-web clients map to UNAVAILABLE, and every other request with a JSON 503.
func writeShuttingDown(w http.ResponseWriter, requestContentType string) {
	// A length the handler declared would truncate the refusal.
	w.Header().Del("Content-Length")
	if !strings.HasPrefix(requestContentType, "application/grpc-web") {
		writeOperationalModeErrorResponse(w, errServerShuttingDown)
		return
	}
	w.Header().Del("Grpc-Status-Details-Bin")
	w.Header().Set("Content-Type", requestContentType)
	w.Header().Set("Grpc-Status", strconv.Itoa(int(codes.Unavailable)))
	w.Header().Set("Grpc-Message", errServerShuttingDown.Error())
	w.WriteHeader(http.StatusOK)
}

type unavailableAfterCancelWriter struct {
	http.ResponseWriter
	inFlight            *inFlightCancel
	requestContentType  string
	responseDecided     bool
	replacedWithRefusal bool
}

// decideResponse runs on the first write or when the handler returns, and
// sends a refusal in place of the handler's response if the cancel already fired.
func (w *unavailableAfterCancelWriter) decideResponse() {
	if w.responseDecided {
		return
	}
	w.responseDecided = true
	if w.inFlight.requestsCtx.Err() == nil {
		return
	}
	w.replacedWithRefusal = true
	w.inFlight.unavailableResponses.Add(1)
	writeShuttingDown(w.ResponseWriter, w.requestContentType)
}

func (w *unavailableAfterCancelWriter) WriteHeader(status int) {
	w.decideResponse()
	if !w.replacedWithRefusal {
		w.ResponseWriter.WriteHeader(status)
	}
}

func (w *unavailableAfterCancelWriter) Write(b []byte) (int, error) {
	w.decideResponse()
	if w.replacedWithRefusal {
		// go-swagger responders panic on a write error, so report the dropped bytes as written.
		return len(b), nil
	}
	return w.ResponseWriter.Write(b)
}

func (w *unavailableAfterCancelWriter) Flush() {
	w.decideResponse()
	if f, ok := w.ResponseWriter.(http.Flusher); ok && !w.replacedWithRefusal {
		f.Flush()
	}
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (w *unavailableAfterCancelWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}
