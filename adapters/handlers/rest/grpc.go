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

package rest

import (
	"github.com/weaviate/weaviate/adapters/handlers/grpc"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
)

func createGrpcServer(state *state.State) *grpc.GRPCServer {
	return grpc.CreateGRPCServer(state)
}

func startGrpcServer(server *grpc.GRPCServer, state *state.State) {
	go func() {
		if err := grpc.StartAndListen(server, state); err != nil {
			state.Logger.WithField("action", "grpc_startup").WithError(err).
				Fatal("failed to start grpc server")
		}
	}()
}
