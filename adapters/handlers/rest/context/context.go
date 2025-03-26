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

package context

import (
	"context"

	"github.com/weaviate/weaviate/entities/models"
)

type contextKey string

func (c contextKey) String() string {
	return string(c)
}

const ctxPrincipalKey = contextKey("principal")

func GetPrincipalFromContext(ctx context.Context) *models.Principal {
	principal := ctx.Value(ctxPrincipalKey)
	if principal == nil {
		return nil
	}

	return principal.(*models.Principal)
}

func AddPrincipalToContext(ctx context.Context, principal *models.Principal) context.Context {
	return context.WithValue(ctx, ctxPrincipalKey, principal)
}
