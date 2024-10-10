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

package monitoring

import (
	"context"
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

// Make sure `GrpcStatsHandler always implements stats.Handler
var _ stats.Handler = &GrpcStatsHandler{}

type key int

const (
	keyMethodName key = 1
	keyRouteName  key = 2

	gRPCTransportLabel = "gRPC"
)

func NewGrpcStatsHandler(inflight *prometheus.GaugeVec, requestSize *prometheus.HistogramVec, responseSize *prometheus.HistogramVec) *GrpcStatsHandler {
	return &GrpcStatsHandler{
		inflightRequests: inflight,
		requestSize:      requestSize,
		responseSize:     responseSize,
	}
}

type GrpcStatsHandler struct {
	inflightRequests *prometheus.GaugeVec

	// in bytes
	requestSize  *prometheus.HistogramVec
	responseSize *prometheus.HistogramVec
}

func (g *GrpcStatsHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	return context.WithValue(ctx, keyMethodName, info.FullMethodName)
}

func (g *GrpcStatsHandler) HandleRPC(ctx context.Context, rpcStats stats.RPCStats) {
	fullMethodName, ok := ctx.Value(keyMethodName).(string)
	if !ok {
		return
	}

	switch s := rpcStats.(type) {
	case *stats.Begin:
		g.inflightRequests.WithLabelValues(gRPCTransportLabel, fullMethodName).Inc()
	case *stats.End:
		g.inflightRequests.WithLabelValues(gRPCTransportLabel, fullMethodName).Dec()
	case *stats.InHeader:
		// Ignore incoming headers.
	case *stats.InPayload:
		g.requestSize.WithLabelValues(gRPCTransportLabel, fullMethodName).Observe(float64(s.WireLength))
	case *stats.InTrailer:
		// Ignore incoming trailers.
	case *stats.OutHeader:
		// Ignore outgoing headers.
	case *stats.OutPayload:
		g.responseSize.WithLabelValues(gRPCTransportLabel, fullMethodName).Observe(float64(s.WireLength))
	case *stats.OutTrailer:
		// Ignore outgoing trailers. OutTrailer doesn't have valid WireLength (there is a deprecated field, always set to 0).
	}
}

func (g *GrpcStatsHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (g *GrpcStatsHandler) HandleConn(_ context.Context, _ stats.ConnStats) {
	// Don't need
}

func UnaryServerInstrument(hist *prometheus.HistogramVec) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		begin := time.Now()
		resp, err := handler(ctx, req)
		observe(hist, info.FullMethod, err, time.Since(begin))
		return resp, err
	}
}

func StreamServerInstrument(hist *prometheus.HistogramVec) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		begin := time.Now()
		err := handler(srv, ss)
		observe(hist, info.FullMethod, err, time.Since(begin))
		return err
	}
}

func observe(hist *prometheus.HistogramVec, method string, err error, duration time.Duration) {
	// hist has following labels
	// method - "gRPC" string
	// route - actual grpc method invoked (e.g: v1/Search)
	// status_code - grpc status codes
	labelValues := []string{
		gRPCTransportLabel,
		method,
		errorToStatus(err),
	}
	hist.WithLabelValues(labelValues...).Observe(duration.Seconds())
}

func errorToStatus(err error) string {
	code := errorToGrpcCode(err)
	return code.String()
}

func errorToGrpcCode(err error) codes.Code {
	if err == nil {
		return codes.OK
	}

	if errors.Is(err, context.Canceled) {
		return codes.Canceled
	}

	type grpcStatus interface {
		GRPCStatus() *status.Status
	}

	var g grpcStatus
	if errors.As(err, &g) {
		st := g.GRPCStatus()
		if st != nil {
			return st.Code()
		}
	}
	return codes.Unknown
}
