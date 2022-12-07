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

package schema

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/sirupsen/logrus"
)

// startupClusterSync tries to determine what - if any - schema migration is
// required at startup. If a node is the first in a cluster the assumption is
// that its state is the truth.
//
// For the n-th node (where n>1) there is a potential for conflict if the
// schemas aren't in sync:
//
// - If Node 1 has a non-nil schema, but Node 2 has a nil-schema, then we can
// consider Node 2 to be a new node that is just joining the cluster. In this
// case, we can copy the state from the existing nodes (if they agree on a
// schema)
//
// - If Node 1 and Node 2 have an identical schema, then we can assume that the
// startup was just an ordinary (re)start of the node. No action is required.
//
// - If Node 1 and Node 2 both have a schema, but they aren't in sync, the
// cluster is broken. This state cannot be automatically recovered from and
// startup needs to fail. Manual intervention would be required in this case.
func (m *Manager) startupClusterSync(ctx context.Context,
	localSchema *State,
) error {
	nodes := m.clusterState.AllNames()
	if len(nodes) <= 1 {
		return m.startupHandleSingleNode(ctx, nodes)
	}

	if isEmpty(localSchema) {
		return m.startupJoinCluster(ctx, localSchema)
	}

	return m.validateSchemaCorruption(ctx, localSchema)
}

// startupHandleSingleNode deals with the case where there is only a single
// node in the cluster. In the vast majority of cases there is nothing to do.
// An edge case would be where the cluster has size=0, or size=1 but the node's
// name is not the local name's node. This would indicate a broken cluster and
// can't be recovered from
func (m *Manager) startupHandleSingleNode(ctx context.Context,
	nodes []string,
) error {
	localName := m.clusterState.LocalName()
	if len(nodes) == 0 {
		return fmt.Errorf("corrupt cluster state: cluster has size=0")
	}

	if nodes[0] != localName {
		return fmt.Errorf("corrupt cluster state: only node in the cluster does not "+
			"match local node name: %v vs %s", nodes, localName)
	}

	m.logger.WithFields(logrusStartupSyncFields()).
		Debug("Only node in the cluster at this point. " +
			"No schema sync necessary.")

	// startup is complete
	return nil
}

// startupJoinCluster migrates the schema for a new node. The assumption is
// that other nodes have schema state and we need to migrate this schema to the
// local node transactionally. In other words, this startup process can not
// occur concurrently with a user-initiated schema update. One of those must
// fail.
//
// There is one edge case: The cluster could consist of multiple nodes which
// are empty. In this case, no migration is required.
func (m *Manager) startupJoinCluster(ctx context.Context,
	localSchema *State,
) error {
	tx, err := m.cluster.BeginTransaction(ctx, ReadSchema, nil, DefaultTxTTL)
	if err != nil {
		if m.clusterSyncImpossibleBecauseRemoteNodeTooOld(err) {
			return nil
		}
		return fmt.Errorf("read schema: open transaction: %w", err)
	}

	// this tx is read-only, so we don't have to worry about aborting it, the
	// close should be the same on both happy and unhappy path
	defer m.cluster.CloseReadTransaction(ctx, tx)

	pl, ok := tx.Payload.(ReadSchemaPayload)
	if !ok {
		return fmt.Errorf("unrecognized tx response payload: %T", tx.Payload)
	}

	// by the time we're here the consensus function has run, so we can be sure
	// that all other nodes agree on this schema.

	if isEmpty(pl.Schema) {
		// already in sync, nothing to do
		return nil
	}

	m.state = *pl.Schema

	m.saveSchema(ctx)

	return nil
}

// validateSchemaCorruption makes sure that - given that all nodes in the
// cluster have a schema - they are in sync. If not the cluster is considered
// broken and needs to be repaired manually
func (m *Manager) validateSchemaCorruption(ctx context.Context,
	localSchema *State,
) error {
	tx, err := m.cluster.BeginTransaction(ctx, ReadSchema, nil, DefaultTxTTL)
	if err != nil {
		if m.clusterSyncImpossibleBecauseRemoteNodeTooOld(err) {
			return nil
		}
		return fmt.Errorf("read schema: open transaction: %w", err)
	}

	// this tx is read-only, so we don't have to worry about aborting it, the
	// close should be the same on both happy and unhappy path
	defer m.cluster.CloseReadTransaction(ctx, tx)

	pl, ok := tx.Payload.(ReadSchemaPayload)
	if !ok {
		return fmt.Errorf("unrecognized tx response payload: %T", tx.Payload)
	}

	if !Equal(localSchema, pl.Schema) {
		localSchemaJSON, err1 := json.Marshal(localSchema)
		consensusSchemaJSON, err2 := json.Marshal(pl.Schema)
		m.logger.WithFields(logrusStartupSyncFields()).WithFields(logrus.Fields{
			"local_schema":         string(localSchemaJSON),
			"remote_schema":        string(consensusSchemaJSON),
			"marhsal_error_local":  err1,
			"marhsal_error_remote": err2,
		}).Errorf("mismatch between local schema and remote (other nodes consensus) schema")

		return fmt.Errorf("corrupt cluster: other nodes have consensus on schema, " +
			"but local node has a different (non-null) schema")
	}

	return nil
}

func logrusStartupSyncFields() logrus.Fields {
	return logrus.Fields{"action": "startup_cluster_schema_sync"}
}

func isEmpty(schema *State) bool {
	if schema.ObjectSchema == nil {
		return true
	}

	if len(schema.ObjectSchema.Classes) == 0 {
		return true
	}

	return false
}

func (m *Manager) clusterSyncImpossibleBecauseRemoteNodeTooOld(err error) bool {
	// string-matching on the error message isn't the cleanest way possible, but
	// unfortunately there's not an easy way to find out, as this check has to
	// work with whatever was already present in v1.16.x
	//
	// in theory we could have used the node api which also returns the versions,
	// however, the node API depends on the DB which depends on the schema
	// manager, so we cannot use them at schema manager startup which happens
	// before db startup.
	//
	// Given that this workaround should only ever be required during a rolling
	// update from v1.16 to v1.17, we can consider this acceptable
	if strings.Contains(err.Error(), "unrecognized schema transaction type") {
		m.logger.WithFields(logrusStartupSyncFields()).
			Info("skipping schema cluster sync because not all nodes in the cluster " +
				"support schema cluster sync yet. To enable schema cluster sync at startup " +
				"make sure all nodes in the cluster run at least v1.17")
		return true
	}

	return false
}
