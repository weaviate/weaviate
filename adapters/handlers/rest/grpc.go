package rest

import (
	"log"

	"github.com/weaviate/weaviate/adapters/handlers/grpc"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
)

func setupGrpc(state *state.State) {
	// TODO: hide behind flag and only start when requested

	port := 50051 // TODO: make configurable
	if err := grpc.StartAndListen(port, state); err != nil {
		// TODO: use proper logger
		log.Fatalf("failed to serve: %v", err)
	}
}
