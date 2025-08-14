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

package read

import (
	"context"

	"github.com/weaviate/weaviate/adapters/handlers/mcp/auth"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

type schemaReader interface {
	GetConsistentSchema(principal *models.Principal, consistency bool) (schema.Schema, error)
	GetConsistentTenants(ctx context.Context, principal *models.Principal, class string, consistency bool, tenants []string) ([]*models.Tenant, error)
}

type WeaviateReader struct {
	auth.Auth

	schemaReader      schemaReader
	defaultCollection string
}

// type schemaManager interface {
// 	AddObject(context.Context, *models.Principal, *models.Object,
// 		*additional.ReplicationProperties) (*models.Object, error)
// }

func NewWeaviateReader(auth *auth.Auth, schemaReader schemaReader) *WeaviateReader {
	return &WeaviateReader{
		defaultCollection: "DefaultCollection",
		schemaReader:      schemaReader,
		Auth:              *auth,
	}
}
