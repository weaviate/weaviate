//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package rest

import (
	grpcHandler "github.com/weaviate/weaviate/adapters/handlers/grpc"
	v1 "github.com/weaviate/weaviate/adapters/handlers/grpc/v1"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"google.golang.org/grpc"
)

func createGrpcServer(state *state.State, shutdownContexts *v1.ShutdownContexts, options ...grpc.ServerOption) *grpc.Server {
	return grpcHandler.CreateGRPCServer(state, shutdownContexts, options...)
}

func startGrpcServer(server *grpc.Server, state *state.State) {
	enterrors.GoWrapper(func() {
		if err := grpcHandler.StartAndListen(server, state); err != nil {
			state.Logger.WithField("action", "grpc_startup").WithError(err).
				Fatal("failed to start grpc server")
		}
	}, state.Logger)
}
