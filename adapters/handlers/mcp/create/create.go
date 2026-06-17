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

	batchManager       batchManager
	logger             logrus.FieldLogger
	writeAccessEnabled func() bool
}

type batchManager interface {
	AddObjects(ctx context.Context, principal *models.Principal,
		objects []*models.Object, fields []*string, repl *additional.ReplicationProperties) (objects.BatchObjects, error)
}

// NewWeaviateCreator creates a new WeaviateCreator. The writeAccessEnabled
// callback is checked at every UpsertObject call so the runtime-configurable
// MCP_SERVER_WRITE_ACCESS_ENABLED flag can be honored without a restart.
func NewWeaviateCreator(auth *auth.Auth, batchManager batchManager, logger logrus.FieldLogger, writeAccessEnabled func() bool) *WeaviateCreator {
	return &WeaviateCreator{
		batchManager:       batchManager,
		Auth:               *auth,
		logger:             logger,
		writeAccessEnabled: writeAccessEnabled,
	}
}

// IsWriteAccessEnabled returns whether write tools are currently allowed to
// execute. Used by tool handlers and by the tools/list filter.
func (c *WeaviateCreator) IsWriteAccessEnabled() bool {
	if c.writeAccessEnabled == nil {
		return false
	}
	return c.writeAccessEnabled()
}
