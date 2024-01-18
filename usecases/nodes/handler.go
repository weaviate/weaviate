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

package nodes

import (
	"context"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

type authorizer interface {
	Authorize(principal *models.Principal, verb, resource string) error
}

type db interface {
	GetNodeStatus(ctx context.Context, className, verbosity string) ([]*models.NodeStatus, error)
}

type Manager struct {
	logger        logrus.FieldLogger
	authorizer    authorizer
	db            db
	schemaManager *schemaUC.Manager
}

func NewManager(logger logrus.FieldLogger, authorizer authorizer,
	db db, schemaManager *schemaUC.Manager,
) *Manager {
	return &Manager{logger, authorizer, db, schemaManager}
}

func (m *Manager) GetNodeStatus(ctx context.Context,
	principal *models.Principal, className string, verbosity string,
) ([]*models.NodeStatus, error) {
	if err := m.authorizer.Authorize(principal, "list", "nodes"); err != nil {
		return nil, err
	}
	return m.db.GetNodeStatus(ctx, className, verbosity)
}
