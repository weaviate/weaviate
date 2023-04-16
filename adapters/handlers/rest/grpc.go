//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package rest

import (
	"log"

	"github.com/weaviate/weaviate/adapters/handlers/grpc"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
)

func setupGrpc(state *state.State) {
	// TODO: hide behind flag and only start when requested

	go func() {
		port := 50051 // TODO: make configurable
		if err := grpc.StartAndListen(port, state); err != nil {
			// TODO: use proper logger
			log.Fatalf("failed to serve: %v", err)
		}
	}()
}
