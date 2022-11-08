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
	"fmt"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/models"
)

// DeleteClass from the schema
func (m *Manager) DeleteClass(ctx context.Context, principal *models.Principal, class string) error {
	err := m.authorizer.Authorize(principal, "delete", "schema/objects")
	if err != nil {
		return err
	}

	return m.deleteClass(ctx, class)
}

func (m *Manager) deleteClass(ctx context.Context, className string) error {
	m.Lock()
	defer m.Unlock()

	tx, err := m.cluster.BeginWriteTransaction(ctx, DeleteClass,
		DeleteClassPayload{className})
	if err != nil {
		// possible causes for errors could be nodes down (we expect every node to
		// the up for a schema transaction) or concurrent transactions from other
		// nodes
		return errors.Wrap(err, "open cluster-wide transaction")
	}

	if err := m.cluster.CommitWriteTransaction(ctx, tx); err != nil {
		return errors.Wrap(err, "commit cluster-wide transaction")
	}

	return m.deleteClassApplyChanges(ctx, className)
}

func (m *Manager) deleteClassApplyChanges(ctx context.Context, className string) error {
	semanticSchema := m.state.SchemaFor()
	classIdx := -1
	for idx, class := range semanticSchema.Classes {
		if class.Class == className {
			classIdx = idx
			break
		}
	}

	if classIdx == -1 {
		return fmt.Errorf("could not find class '%s'", className)
	}

	semanticSchema.Classes[classIdx] = semanticSchema.Classes[len(semanticSchema.Classes)-1]
	semanticSchema.Classes[len(semanticSchema.Classes)-1] = nil // to prevent leaking this pointer.
	semanticSchema.Classes = semanticSchema.Classes[:len(semanticSchema.Classes)-1]

	err := m.saveSchema(ctx)
	if err != nil {
		return err
	}

	return m.migrator.DropClass(ctx, className)
	// TODO gh-846: rollback state update if migration fails
}
