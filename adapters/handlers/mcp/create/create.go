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

package create

import (
	"context"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/auth"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/objects"
)

type WeaviateCreator struct {
	auth.Auth

	batchManager batchManager
	logger       logrus.FieldLogger
}

type batchManager interface {
	AddObjects(ctx context.Context, principal *models.Principal,
		objects []*models.Object, fields []*string, repl *additional.ReplicationProperties) (objects.BatchObjects, error)
}

func NewWeaviateCreator(auth *auth.Auth, batchManager batchManager, logger logrus.FieldLogger) *WeaviateCreator {
	return &WeaviateCreator{
		batchManager: batchManager,
		Auth:         *auth,
		logger:       logger,
	}
}
