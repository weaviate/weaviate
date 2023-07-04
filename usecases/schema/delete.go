//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package schema

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
)

// DeleteClass from the schema
func (m *Manager) DeleteClass(ctx context.Context, principal *models.Principal, class string, force bool) error {
	err := m.Authorizer.Authorize(principal, "delete", "schema/objects")
	if err != nil {
		return err
	}

	return m.deleteClass(ctx, class, force)
}

func (m *Manager) deleteClass(ctx context.Context, className string, force bool) error {
	m.Lock()
	defer m.Unlock()

	tx, err := m.cluster.BeginTransaction(ctx, DeleteClass,
		DeleteClassPayload{className, force}, DefaultTxTTL)
	if err != nil {
		// possible causes for errors could be nodes down (we expect every node to
		// be up for a schema transaction) or concurrent transactions from other
		// nodes
		return errors.Wrap(err, "open cluster-wide transaction")
	}

	if err := m.cluster.CommitWriteTransaction(ctx, tx); err != nil {
		// Only log the commit error, but do not abort the changes locally. Once
		// we've told others to commit, we also need to commit ourselves!
		//
		// The idea is that if we abort our changes we are guaranteed to create an
		// inconsistency as soon as any other node honored the commit. This would
		// for example be the case in a 3-node cluster where node 1 is the
		// coordinator, node 2 honored the commit and node 3 died during the commit
		// phase.
		//
		// In this scenario it is far more desirable to make sure that node 1 and
		// node 2 stay in sync, as node 3 - who may or may not have missed the
		// update - can use a local WAL from the first TX phase to replay any
		// missing changes once it's back.
		m.logger.WithError(err).Errorf("not every node was able to commit")
	}

	return m.deleteClassApplyChanges(ctx, className, force)
}

func (m *Manager) deleteClassApplyChanges(ctx context.Context,
	className string, force bool,
) error {
	classIdx := -1
	var sch *models.Schema
	m.schemaCache.RLockGuard(func() error {
		sch = m.schemaCache.ObjectSchema
		for idx, class := range sch.Classes {
			if class.Class == className {
				classIdx = idx
				break
			}
		}
		return nil
	})

	if classIdx == -1 && !force {
		return fmt.Errorf("could not find class '%s'", className)
	}

	if classIdx > -1 {
		// make sure not to delete another class if the force flag is set, but the class does not exist
		m.schemaCache.LockGuard(func() {
			sch.Classes[classIdx] = sch.Classes[len(sch.Classes)-1]
			sch.Classes[len(sch.Classes)-1] = nil // to prevent leaking this pointer.
			sch.Classes = sch.Classes[:len(sch.Classes)-1]
		})
	}

	err := m.migrator.DropClass(ctx, className)
	if err != nil {
		if !force {
			return err
		}

		m.logger.WithError(err).
			Errorf("ignoring class delete error because force is set")
	}

	m.schemaCache.LockGuard(func() { delete(m.schemaCache.ShardingState, className) })
	if err := m.repo.DeleteClass(ctx, className); err != nil {
		return err
	}
	m.logger.
		WithField("action", "schema_delete_class").
		Debugf("delete class %q from schema", className)

	m.triggerSchemaUpdateCallbacks()

	return nil
}
