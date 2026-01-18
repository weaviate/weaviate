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

package create

import (
	"context"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/auth"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/objects"
)

type WeaviateCreator struct {
	auth.Auth

	objectsManager    objectsManager
	schemaManager     schemaManager
	batchManager      batchManager
	defaultCollection string
}

type objectsManager interface {
	AddObject(context.Context, *models.Principal, *models.Object,
		*additional.ReplicationProperties) (*models.Object, error)
	UpdateObject(context.Context, *models.Principal, string, strfmt.UUID, *models.Object,
		*additional.ReplicationProperties) (*models.Object, error)
	GetObject(context.Context, *models.Principal, string, strfmt.UUID, additional.Properties,
		*additional.ReplicationProperties, string) (*models.Object, error)
}

type schemaManager interface {
	AddClass(ctx context.Context, principal *models.Principal, class *models.Class) (*models.Class, uint64, error)
}

type batchManager interface {
	DeleteObjects(ctx context.Context, principal *models.Principal, match *models.BatchDeleteMatch,
		deletionTimeUnixMilli *int64, dryRun *bool, output *string,
		repl *additional.ReplicationProperties, tenant string) (*objects.BatchDeleteResponse, error)
}

func NewWeaviateCreator(auth *auth.Auth, objectsManager objectsManager, schemaManager schemaManager, batchManager batchManager) *WeaviateCreator {
	return &WeaviateCreator{
		defaultCollection: "DefaultCollection",
		objectsManager:    objectsManager,
		schemaManager:     schemaManager,
		batchManager:      batchManager,
		Auth:              *auth,
	}
}
