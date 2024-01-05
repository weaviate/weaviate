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

package v1

import (
	"context"
	"strings"

	"github.com/weaviate/weaviate/entities/models"
	"google.golang.org/grpc/metadata"
)

// This should probably be run as part of a middleware. In the initial gRPC
// implementation there is only a single endpoint, so it's fine to run this
// straight from the endpoint. But the moment we add a second endpoint, this
// should be called from a central place. This way we can make sure it's
// impossible to forget to add it to a new endpoint.
func (s *Service) principalFromContext(ctx context.Context) (*models.Principal, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return s.tryAnonymous()
	}

	// the grpc library will lowercase all md keys, so we need to make sure to
	// check a lowercase key
	authValue, ok := md["authorization"]
	if !ok {
		return s.tryAnonymous()
	}

	if len(authValue) == 0 {
		return s.tryAnonymous()
	}

	if !strings.HasPrefix(authValue[0], "Bearer ") {
		return s.tryAnonymous()
	}

	token := strings.TrimPrefix(authValue[0], "Bearer ")
	return s.authComposer(token, nil)
}

func (s *Service) tryAnonymous() (*models.Principal, error) {
	if s.allowAnonymousAccess {
		return nil, nil
	}

	return s.authComposer("", nil)
}
