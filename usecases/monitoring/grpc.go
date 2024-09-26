package monitoring

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/stats"
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
