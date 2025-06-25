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
	"time"

	"github.com/weaviate/weaviate/usecases/auth/authorization/filter"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"

	"github.com/weaviate/weaviate/entities/verbosity"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

const GetNodeStatusTimeout = 30 * time.Second

type db interface {
	GetNodeStatus(ctx context.Context, className, shardName, verbosity string) ([]*models.NodeStatus, error)
	GetNodeStatistics(ctx context.Context) ([]*models.Statistics, error)
}

type Manager struct {
	logger        logrus.FieldLogger
	authorizer    authorization.Authorizer
	db            db
	schemaManager *schemaUC.Manager
	rbacconfig    rbacconf.Config
}

func NewManager(logger logrus.FieldLogger, authorizer authorization.Authorizer,
	db db, schemaManager *schemaUC.Manager, rbacconfig rbacconf.Config,
) *Manager {
	return &Manager{logger, authorizer, db, schemaManager, rbacconfig}
}

// GetNodeStatus aggregates the status across all nodes. It will try for a
// maximum of the configured timeout, then mark nodes as timed out.
func (m *Manager) GetNodeStatus(ctx context.Context,
	principal *models.Principal, className, shardName, verbosityString string,
) ([]*models.NodeStatus, error) {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, GetNodeStatusTimeout)
	defer cancel()

	// filter output after getting results if info about all shards is requested
	filterOutput := verbosityString == verbosity.OutputVerbose && className == "" && m.rbacconfig.Enabled

	if !filterOutput {
		if err := m.authorizer.Authorize(ctx, principal, authorization.READ, authorization.Nodes(verbosityString, className)...); err != nil {
			return nil, err
		}
	}

	status, err := m.db.GetNodeStatus(ctxWithTimeout, className, shardName, verbosityString)
	if err != nil {
		return nil, err
	}

	if filterOutput {
		resourceFilter := filter.New[*models.NodeShardStatus](m.authorizer, m.rbacconfig)

		for i, nodeS := range status {
			status[i].Shards = resourceFilter.Filter(
				ctx,
				m.logger,
				principal,
				nodeS.Shards,
				authorization.READ,
				func(shard *models.NodeShardStatus) string {
					return authorization.Nodes(verbosityString, shard.Class)[0]
				},
			)
		}
	}

	return status, nil
}

func (m *Manager) GetNodeStatistics(ctx context.Context,
	principal *models.Principal,
) ([]*models.Statistics, error) {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, GetNodeStatusTimeout)
	defer cancel()

	if err := m.authorizer.Authorize(ctx, principal, authorization.READ, authorization.Cluster()); err != nil {
		return nil, err
	}
	return m.db.GetNodeStatistics(ctxWithTimeout)
}
