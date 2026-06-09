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
	"net/http"
	"strings"

	"connectrpc.com/vanguard/vanguardgrpc"
	"github.com/rs/cors"
	"google.golang.org/grpc"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
)

// NewHandler transcodes grpc-web/Connect requests into calls against the
// existing gRPC server, reusing its interceptor chain (auth, maintenance mode).
// It carries its own CORS layer because grpc-web requests bypass the REST
// middleware chain, and browsers need the gRPC trailer headers exposed to read
// the RPC status.
func NewHandler(grpcServer *grpc.Server, state *state.State) (http.Handler, error) {
	transcoder, err := vanguardgrpc.NewTranscoder(grpcServer)
	if err != nil {
		return nil, fmt.Errorf("build grpc-web transcoder: %w", err)
	}
	corsCfg := state.ServerConfig.Config.CORS
	return cors.New(cors.Options{
		AllowedOrigins:   strings.Split(corsCfg.AllowOrigin, ","),
		AllowedMethods:   []string{http.MethodPost},
		AllowedHeaders:   []string{"Content-Type", "X-Grpc-Web", "X-User-Agent", "Grpc-Timeout", "Authorization", "X-Weaviate-Client", "X-Weaviate-Cluster-Url"},
		ExposedHeaders:   []string{"Grpc-Status", "Grpc-Message", "Grpc-Status-Details-Bin"},
		AllowCredentials: true,
	}).Handler(transcoder), nil
}

// Mount routes requests under prefix to grpcWeb (with prefix stripped, so the
// transcoder sees the canonical /<pkg>.<Service>/<Method> path) and everything
// else to next. The grpc-web branch deliberately sits outside the REST
// middleware chain: the gRPC server's interceptors enforce auth and maintenance
// mode, whereas the REST operational-mode gate would misclassify /weaviate.v1.*
// paths.
func Mount(prefix string, grpcWeb, next http.Handler) http.Handler {
	stripped := http.StripPrefix(prefix, grpcWeb)
	prefixSlash := prefix + "/"
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == prefix || strings.HasPrefix(r.URL.Path, prefixSlash) {
			stripped.ServeHTTP(w, r)
			return
		}
		next.ServeHTTP(w, r)
	})
}
