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

package search

import (
	"context"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/auth"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/schema"
)

type WeaviateSearcher struct {
	auth.Auth

	traverser         traverser
	schemaManager     *schema.Manager
	namespacesEnabled bool
	logger            logrus.FieldLogger
}

type traverser interface {
	GetClass(ctx context.Context, principal *models.Principal, params dto.GetParams) ([]any, error)
}

func NewWeaviateSearcher(auth *auth.Auth, traverser traverser, schemaManager *schema.Manager, namespacesEnabled bool, logger logrus.FieldLogger) *WeaviateSearcher {
	return &WeaviateSearcher{
		traverser:         traverser,
		schemaManager:     schemaManager,
		namespacesEnabled: namespacesEnabled,
		Auth:              *auth,
		logger:            logger,
	}
}
