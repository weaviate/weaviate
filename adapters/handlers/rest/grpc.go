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
	"time"

	"github.com/sirupsen/logrus"

	grpcHandler "github.com/weaviate/weaviate/adapters/handlers/grpc"
	"github.com/weaviate/weaviate/adapters/handlers/grpc/v1/batch"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/telemetry"
	"google.golang.org/grpc"
)

func createGrpcServer(state *state.State, clientTracker *telemetry.ClientTracker, integrationTracker *telemetry.IntegrationTracker, options ...grpc.ServerOption) (*grpc.Server, batch.Drain) {
	return grpcHandler.CreateGRPCServer(state, clientTracker, integrationTracker, options...)
}

func startGrpcServer(server *grpc.Server, state *state.State) {
	enterrors.GoWrapper(func() {
		if err := grpcHandler.StartAndListen(server, state); err != nil {
			state.Logger.WithField("action", "grpc_startup").
				Fatalf("failed to start grpc server: %v", err)
		}
	}, state.Logger)
}

// startGrpcStop refuses grpc-web calls, which a graceful stop answers with a
// non-retryable Unknown, then starts the graceful stop and forces Stop
// stopTimeout later. wait only joins it, and a handler ignoring its ctx may outlive it.
func startGrpcStop(server *grpc.Server, refuseGrpcWeb func(), stopTimeout time.Duration,
	logger logrus.FieldLogger,
) (wait func()) {
	refuseGrpcWeb()
	stopped := make(chan struct{})
	enterrors.GoWrapper(func() {
		server.GracefulStop()
		close(stopped)
	}, logger)
	done := make(chan struct{})
	enterrors.GoWrapper(func() {
		defer close(done)
		deadline := time.NewTimer(stopTimeout)
		defer deadline.Stop()
		select {
		case <-stopped:
		case <-deadline.C:
			logger.Warn("grpc graceful stop timed out, forcing stop")
			server.Stop()
		}
	}, logger)
	return func() { <-done }
}
