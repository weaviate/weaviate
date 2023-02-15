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
	err := m.authorizer.Authorize(principal, "delete", "schema/objects")
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
		// the up for a schema transaction) or concurrent transactions from other
		// nodes
		return errors.Wrap(err, "open cluster-wide transaction")
	}

	if err := m.cluster.CommitWriteTransaction(ctx, tx); err != nil {
		return errors.Wrap(err, "commit cluster-wide transaction")
	}

	return m.deleteClassApplyChanges(ctx, className, force)
}

func (m *Manager) deleteClassApplyChanges(ctx context.Context,
	className string, force bool,
) error {
	sch := m.state.ObjectSchema
	classIdx := -1
	for idx, class := range sch.Classes {
		if class.Class == className {
			classIdx = idx
			break
		}
	}

	if classIdx == -1 && !force {
		return fmt.Errorf("could not find class '%s'", className)
	}

	if classIdx > -1 {
		// make sure not to delete another class if the force flag is set, but the class does not exist
		sch.Classes[classIdx] = sch.Classes[len(sch.Classes)-1]
		sch.Classes[len(sch.Classes)-1] = nil // to prevent leaking this pointer.
		sch.Classes = sch.Classes[:len(sch.Classes)-1]
	}

	err := m.saveSchema(ctx)
	if err != nil {
		return err
	}

	err = m.migrator.DropClass(ctx, className)
	if err != nil {
		if !force {
			return err
		}

		m.logger.WithError(err).
			Errorf("ignoring class delete error because force is set")
	}

	m.shardingStateLock.Lock()
	delete(m.state.ShardingState, className)
	m.shardingStateLock.Unlock()
	err = m.saveSchema(ctx)
	if err != nil {
		return err
	}

	return nil
}
