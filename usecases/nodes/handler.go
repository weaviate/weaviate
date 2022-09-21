//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package nodes

import (
	"context"

	"github.com/semi-technologies/weaviate/entities/models"
	schemaUC "github.com/semi-technologies/weaviate/usecases/schema"
	"github.com/sirupsen/logrus"
)

type authorizer interface {
	Authorize(principal *models.Principal, verb, resource string) error
}

type db interface {
	GetNodeStatuses(ctx context.Context) ([]*models.NodeStatus, error)
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

func (m *Manager) GetNodeStatuses(ctx context.Context,
	principal *models.Principal,
) ([]*models.NodeStatus, error) {
	if err := m.authorizer.Authorize(principal, "list", "nodes"); err != nil {
		return nil, err
	}
	return m.db.GetNodeStatuses(ctx)
}
