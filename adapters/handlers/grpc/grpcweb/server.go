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

// Package grpcweb exposes the gRPC API over grpc-web/Connect so browsers can
// reach it directly. The transcoder is mounted on the existing REST port under
// a path prefix (see Mount) instead of a dedicated listener, so operators do
// not have to expose a second port.
package grpcweb

import (
	"fmt"
	"math"
	"net/http"
	"strings"

	"connectrpc.com/vanguard"
	"connectrpc.com/vanguard/vanguardgrpc"
	"github.com/rs/cors"
	"google.golang.org/grpc"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/usecases/config"
)

// NewHandler transcodes grpc-web/Connect requests into calls against the
// existing gRPC server. It carries its own CORS layer (grpc-web is routed
// outside the REST middleware chain; see Mount) and exposes the gRPC trailer
// headers so browsers can read the RPC status.
func NewHandler(grpcServer *grpc.Server, state *state.State) (http.Handler, error) {
	opts := []vanguard.ServiceOption{}
	opts = append(opts, vanguard.WithMaxMessageBufferBytes(msgBufferLimit(state.ServerConfig.Config.GRPC.MaxMsgSize)))
	transcoder, err := vanguardgrpc.NewTranscoder(grpcServer, vanguard.WithDefaultServiceOptions(opts...))
	if err != nil {
		return nil, fmt.Errorf("build grpc-web transcoder: %w", err)
	}
	return cors.New(corsOptions(state.ServerConfig.Config.CORS)).Handler(transcoder), nil
}

// corsOptions builds the CORS config for the grpc-web handler: the
// operator-configured allowlists plus the grpc-web protocol headers browsers
// send, with the gRPC trailers exposed so the browser can read the RPC status.
func corsOptions(cfg config.CORS) cors.Options {
	// grpc-web/Connect protocol headers not present in the general REST
	// allowlist. Connect-Protocol-Version is mandatory for Connect-JSON clients
	headers := append([]string{
		"X-Grpc-Web", "X-User-Agent", "Grpc-Timeout",
		"Connect-Protocol-Version", "Connect-Timeout-Ms",
		"X-Weaviate-Client",
	}, splitTrim(cfg.AllowHeaders)...)
	return cors.Options{
		AllowedOrigins: splitTrim(cfg.AllowOrigin),
		AllowedMethods: []string{http.MethodPost}, // grpc-web is POST-only
		AllowedHeaders: headers,
		ExposedHeaders: []string{"Grpc-Status", "Grpc-Message", "Grpc-Status-Details-Bin"},
		// No CORS credentials mode: Weaviate auth is a bearer token in the
		// Authorization header (not cookies), so it buys nothing, and
		// Allow-Credentials:true with a wildcard Allow-Origin is a combo
		// browsers reject. Matches the REST handler's posture.
		AllowCredentials: false,
	}
}

func msgBufferLimit(maxMsgSize int) uint32 {
	if maxMsgSize <= 0 || uint64(maxMsgSize) > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(maxMsgSize)
}

// splitTrim splits a comma-separated config value and trims each element.
// The trim is critical: DefaultCORSAllowHeaders is ", "-separated and
// rs/cors matches header names verbatim, so a leading space on " Authorization"
// would silently fail the browser's preflight and block authenticated requests.
func splitTrim(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// Mount routes requests under prefix to grpcWeb (with prefix stripped, so the
// transcoder sees the canonical /<pkg>.<Service>/<Method> path) and everything
// else to next. The grpc-web branch deliberately sits outside the REST
// middleware chain: the gRPC server's interceptors enforce auth and maintenance
// mode, whereas the REST operational-mode gate would misclassify /weaviate.v1.*
// paths.
//
// enabled is consulted per request, but only for prefix-matching paths, so a
// runtime toggle changes routing without a restart while plain REST requests pay
// nothing for the check. When it returns false, grpc-web paths fall through to
// next (the REST handler) instead of being served.
func Mount(prefix string, grpcWeb, next http.Handler, enabled func() bool) http.Handler {
	stripped := http.StripPrefix(prefix, grpcWeb)
	prefixSlash := prefix + "/"
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == prefix || strings.HasPrefix(r.URL.Path, prefixSlash) {
			if enabled() {
				stripped.ServeHTTP(w, r)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}
