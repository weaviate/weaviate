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

type db interface {
	GetNodeStatus(ctx context.Context, className, shardName, verbosity string) ([]*models.NodeStatus, error)
	GetNodeStatistics(ctx context.Context) ([]*models.Statistics, error)
}

type Manager struct {
	logger                 logrus.FieldLogger
	authorizer             authorization.Authorizer
	db                     db
	schemaManager          *schemaUC.Manager
	rbacconfig             rbacconf.Config
	minimumInternalTimeout time.Duration
}

func NewManager(logger logrus.FieldLogger, authorizer authorization.Authorizer,
	db db, schemaManager *schemaUC.Manager, rbacconfig rbacconf.Config, minimumInternalTimeout time.Duration,
) *Manager {
	return &Manager{logger, authorizer, db, schemaManager, rbacconfig, minimumInternalTimeout}
}

// GetNodeStatus aggregates the status across all nodes. It will try for a
// maximum of the configured timeout, then mark nodes as timed out.
func (m *Manager) GetNodeStatus(ctx context.Context,
	principal *models.Principal, className, shardName, verbosityString string,
) ([]*models.NodeStatus, error) {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, m.minimumInternalTimeout)
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

		// Node-wide Stats and BatchStats are operator-only. Gate on the minimal
		// grant, not on "shards trimmed?": an empty node has nothing to trim, yet
		// its node-wide signal still isn't a scoped caller's to see.
		keepNodeWide := m.authorizer.AuthorizeSilent(ctx, principal, authorization.READ,
			authorization.Nodes(verbosity.OutputMinimal)...) == nil

		for i, nodeS := range status {
			status[i].Shards = resourceFilter.Filter(
				ctx,
				principal,
				nodeS.Shards,
				authorization.READ,
				func(shard *models.NodeShardStatus) string {
					return authorization.Nodes(verbosityString, shard.Class)[0]
				},
			)
			if keepNodeWide {
				continue
			}
			// Scoped caller: recompute Stats from retained shards, drop BatchStats.
			if status[i].Stats != nil {
				var objects int64
				for _, shard := range status[i].Shards {
					objects += shard.ObjectCount
				}
				status[i].Stats.ObjectCount = objects
				status[i].Stats.ShardCount = int64(len(status[i].Shards))
			}
			status[i].BatchStats = nil
		}
	} else if className != "" && verbosityString == verbosity.OutputVerbose && m.rbacconfig.Enabled {
		// The DB scopes Shards/Stats to the class, but BatchStats stays node-wide
		// (global ingest rate/queue). Drop it for a class-scoped caller, keeping
		// it only for an operator who also holds the node-wide minimal view.
		if err := m.authorizer.AuthorizeSilent(ctx, principal, authorization.READ, authorization.Nodes(verbosity.OutputMinimal)...); err != nil {
			for i := range status {
				status[i].BatchStats = nil
			}
		}
	}

	return status, nil
}

func (m *Manager) GetNodeStatistics(ctx context.Context,
	principal *models.Principal,
) ([]*models.Statistics, error) {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, m.minimumInternalTimeout)
	defer cancel()

	if err := m.authorizer.Authorize(ctx, principal, authorization.READ, authorization.Cluster()); err != nil {
		return nil, err
	}
	return m.db.GetNodeStatistics(ctxWithTimeout)
}
