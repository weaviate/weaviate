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

	"github.com/weaviate/weaviate/adapters/handlers/mcp/auth"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
)

type WeaviateCreator struct {
	auth.Auth

	objectsManager    objectsManager
	defaultCollection string
}

type objectsManager interface {
	AddObject(context.Context, *models.Principal, *models.Object,
		*additional.ReplicationProperties) (*models.Object, error)
}

func NewWeaviateCreator(auth *auth.Auth, objectsManager objectsManager) *WeaviateCreator {
	return &WeaviateCreator{
		defaultCollection: "DefaultCollection",
		objectsManager:    objectsManager,
		Auth:              *auth,
	}
}
