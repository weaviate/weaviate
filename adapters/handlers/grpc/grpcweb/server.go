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

package grpcweb

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"strings"

	"connectrpc.com/vanguard/vanguardgrpc"
	"github.com/rs/cors"
	"google.golang.org/grpc"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
)

type Server struct {
	http *http.Server
}

func NewServer(grpcServer *grpc.Server, state *state.State) *Server {
	transcoder, err := vanguardgrpc.NewTranscoder(grpcServer)
	if err != nil {
		state.Logger.WithField("action", "grpc_web_startup").
			Fatalf("build grpc-web transcoder: %s", err)
	}
	corsCfg := state.ServerConfig.Config.CORS
	httpServer := &http.Server{
		Handler: cors.New(cors.Options{
			AllowedOrigins:   strings.Split(corsCfg.AllowOrigin, ","),
			AllowedMethods:   []string{http.MethodPost},
			AllowedHeaders:   []string{"Content-Type", "X-Grpc-Web", "X-User-Agent", "Grpc-Timeout", "Authorization", "X-Weaviate-Client", "X-Weaviate-Cluster-Url"},
			ExposedHeaders:   []string{"Grpc-Status", "Grpc-Message", "Grpc-Status-Details-Bin"},
			AllowCredentials: true,
		}).Handler(transcoder),
	}
	if len(state.ServerConfig.Config.GRPC.CertFile) > 0 || len(state.ServerConfig.Config.GRPC.KeyFile) > 0 {
		cert, err := tls.LoadX509KeyPair(
			state.ServerConfig.Config.GRPC.CertFile,
			state.ServerConfig.Config.GRPC.KeyFile,
		)
		if err != nil {
			state.Logger.WithField("action", "grpc_web_startup").
				Fatalf("load grpc-web TLS cert: %s", err)
		}
		httpServer.TLSConfig = &tls.Config{Certificates: []tls.Certificate{cert}}
	} else {
		protocols := http.Protocols{}
		protocols.SetHTTP1(true)
		protocols.SetUnencryptedHTTP2(true)
		httpServer.Protocols = &protocols
	}
	return &Server{http: httpServer}
}

func (s *Server) Serve(state *state.State) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", state.ServerConfig.Config.GRPC.WebPort))
	if err != nil {
		return err
	}
	state.Logger.WithField("action", "grpc_web_startup").
		Infof("grpc-web server listening at %v", lis.Addr())
	if s.http.TLSConfig != nil {
		return s.http.ServeTLS(lis, "", "")
	}
	return s.http.Serve(lis)
}

func (s *Server) Close(ctx context.Context) error {
	return s.http.Shutdown(ctx)
}
