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

package schema

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// MergeClassObjectProperty of an existing Class
// Merges NestedProperties of incoming object/object[] property into existing one
func (m *Manager) MergeClassObjectProperty(ctx context.Context, principal *models.Principal,
	class string, property *models.Property,
) error {
	err := m.Authorizer.Authorize(principal, "update", "schema/objects")
	if err != nil {
		return err
	}

	return m.mergeClassObjectProperty(ctx, class, property)
}

func (m *Manager) mergeClassObjectProperty(ctx context.Context,
	className string, prop *models.Property,
) error {
	m.Lock()
	defer m.Unlock()

	class, err := m.schemaCache.readOnlyClass(className)
	if err != nil {
		return err
	}
	prop.Name = schema.LowercaseFirstLetter(prop.Name)

	// reuse setDefaults/validation/migrate methods coming from add property
	// (empty existing names map, to validate existing updated property)
	// TODO nested - refactor / cleanup setDefaults/validation/migrate methods
	if err := m.setNewPropDefaults(class, prop); err != nil {
		return err
	}
	if err := m.validateProperty(prop, className, map[string]bool{}, false); err != nil {
		return err
	}
	// migrate only after validation in completed
	migratePropertySettings(prop)

	tx, err := m.cluster.BeginTransaction(ctx, mergeObjectProperty,
		MergeObjectPropertyPayload{className, prop}, DefaultTxTTL)
	if err != nil {
		// possible causes for errors could be nodes down (we expect every node to
		// the up for a schema transaction) or concurrent transactions from other
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

	return m.mergeClassObjectPropertyApplyChanges(ctx, className, prop)
}

func (m *Manager) mergeClassObjectPropertyApplyChanges(ctx context.Context,
	className string, prop *models.Property,
) error {
	class, err := m.schemaCache.mergeObjectProperty(className, prop)
	if err != nil {
		return err
	}
	metadata, err := json.Marshal(&class)
	if err != nil {
		return fmt.Errorf("marshal class %s: %w", className, err)
	}
	m.logger.
		WithField("action", "schema.update_object_property").
		Debug("saving updated schema to configuration store")
	err = m.repo.UpdateClass(ctx, ClassPayload{Name: className, Metadata: metadata})
	if err != nil {
		return err
	}
	m.triggerSchemaUpdateCallbacks()

	// TODO nested - implement MergeObjectProperty (needed for indexing/filtering)
	// will result in a mismatch between schema and index if function below fails
	// return m.migrator.MergeObjectProperty(ctx, className, prop)
	return nil
}
