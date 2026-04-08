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

package read

import (
	"context"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/auth"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/objects"
)

type schemaReader interface {
	GetConsistentSchema(ctx context.Context, principal *models.Principal, consistency bool) (schema.Schema, error)
	GetConsistentTenants(ctx context.Context, principal *models.Principal, class string, consistency bool, tenants []string) ([]*models.Tenant, error)
}

type objectsManager interface {
	GetObject(ctx context.Context, principal *models.Principal, class string, id strfmt.UUID,
		additional additional.Properties, replProps *additional.ReplicationProperties, tenant string) (*models.Object, error)
	GetObjects(ctx context.Context, principal *models.Principal, offset *int64, limit *int64,
		sort *string, order *string, after *string, addl additional.Properties, tenant string) ([]*models.Object, error)
	Query(ctx context.Context, principal *models.Principal, params *objects.QueryParams) ([]*models.Object, *objects.Error)
}

type WeaviateReader struct {
	auth.Auth

	schemaReader   schemaReader
	objectsManager objectsManager
	logger         logrus.FieldLogger
}

func NewWeaviateReader(auth *auth.Auth, schemaReader schemaReader, objectsManager objectsManager, logger logrus.FieldLogger) *WeaviateReader {
	return &WeaviateReader{
		schemaReader:   schemaReader,
		objectsManager: objectsManager,
		Auth:           *auth,
		logger:         logger,
	}
}
