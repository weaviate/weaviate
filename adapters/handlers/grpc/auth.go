package grpc

import (
	"context"
	"fmt"
	"strings"

	"github.com/davecgh/go-spew/spew"
	"github.com/weaviate/weaviate/entities/models"
	"google.golang.org/grpc/metadata"
)

// This should probably be run as part of a middleware. In the initial gRPC
// implementation there is only a single endpoint, so it's fine to run this
// straight from the endpoint. But the moment we add a second endpoint, this
// should be called from a central place. This way we can make sure it's
// impossible to forget to add it to a new endpoint.
func (s *Server) principalFromContext(ctx context.Context) (*models.Principal, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		fmt.Println("no metadata")
		return s.authComposer("", nil)
	}
	spew.Dump(md)

	// the grpc library will lowercase all md keys, so we need to make sure to
	// check a lowercase key
	authValue, ok := md["authorization"]
	if !ok {
		return s.authComposer("", nil)
	}

	if len(authValue) == 0 {
		return s.authComposer("", nil)
	}

	if !strings.HasPrefix(authValue[0], "Bearer ") {
		return s.authComposer("", nil)
	}

	token := strings.TrimPrefix(authValue[0], "Bearer ")
	return s.authComposer(token, nil)
}
