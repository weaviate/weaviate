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

package auth

import (
	"context"
	"strings"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication/composer"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"
)

type Handler struct {
	allowAnonymousAccess bool
	authComposer         composer.TokenFunc
}

func NewHandler(allowAnonymousAccess bool, authComposer composer.TokenFunc) *Handler {
	return &Handler{
		allowAnonymousAccess: allowAnonymousAccess,
		authComposer:         authComposer,
	}
}

// This should probably be run as part of a middleware. In the initial gRPC
// implementation there is only a single endpoint, so it's fine to run this
// straight from the endpoint. But the moment we add a second endpoint, this
// should be called from a central place. This way we can make sure it's
// impossible to forget to add it to a new endpoint.
func (h *Handler) PrincipalFromContext(ctx context.Context) (*models.Principal, error) {
	ctx, span := otel.Tracer("weaviate-search").Start(ctx, "handler.PrincipalFromContext",
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer span.End()

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return h.tryAnonymous()
	}

	// the grpc library will lowercase all md keys, so we need to make sure to
	// check a lowercase key
	authValue, ok := md["authorization"]
	if !ok {
		return h.tryAnonymous()
	}

	if len(authValue) == 0 {
		return h.tryAnonymous()
	}

	if !strings.HasPrefix(authValue[0], "Bearer ") {
		return h.tryAnonymous()
	}

	token := strings.TrimPrefix(authValue[0], "Bearer ")
	return h.authComposer(token, nil)
}

func (h *Handler) tryAnonymous() (*models.Principal, error) {
	if h.allowAnonymousAccess {
		return nil, nil
	}

	return h.authComposer("", nil)
}
