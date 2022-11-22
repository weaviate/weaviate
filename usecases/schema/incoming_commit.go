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

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/usecases/cluster"
)

func (m *Manager) handleCommit(ctx context.Context, tx *cluster.Transaction) error {
	switch tx.Type {
	case AddClass:
		return m.handleAddClassCommit(ctx, tx)
	case AddProperty:
		return m.handleAddPropertyCommit(ctx, tx)
	case DeleteClass:
		return m.handleDeleteClassCommit(ctx, tx)
	case UpdateClass:
		return m.handleUpdateClassCommit(ctx, tx)
	default:
		return errors.Errorf("unrecognized commit type %q", tx.Type)
	}
}

func (m *Manager) handleTxResponse(ctx context.Context,
	tx *cluster.Transaction,
) error {
	switch tx.Type {
	case ReadSchema:
		tx.Payload = ReadSchemaPayload{
			Schema: &m.state,
		}
		return nil
	// TODO
	default:
		// silently ignore. Not all types support responses
		return nil
	}
}

func (m *Manager) handleAddClassCommit(ctx context.Context,
	tx *cluster.Transaction,
) error {
	m.Lock()
	defer m.Unlock()

	pl, ok := tx.Payload.(AddClassPayload)
	if !ok {
		return errors.Errorf("expected commit payload to be AddClassPayload, but got %T",
			tx.Payload)
	}

	err := m.parseShardingConfig(ctx, pl.Class)
	if err != nil {
		return err
	}

	err = m.parseVectorIndexConfig(ctx, pl.Class)
	if err != nil {
		return err
	}

	pl.State.SetLocalName(m.clusterState.LocalName())
	return m.addClassApplyChanges(ctx, pl.Class, pl.State)
}

func (m *Manager) handleAddPropertyCommit(ctx context.Context,
	tx *cluster.Transaction,
) error {
	m.Lock()
	defer m.Unlock()

	pl, ok := tx.Payload.(AddPropertyPayload)
	if !ok {
		return errors.Errorf("expected commit payload to be AddPropertyPayload, but got %T",
			tx.Payload)
	}

	return m.addClassPropertyApplyChanges(ctx, pl.ClassName, pl.Property)
}

func (m *Manager) handleDeleteClassCommit(ctx context.Context,
	tx *cluster.Transaction,
) error {
	m.Lock()
	defer m.Unlock()

	pl, ok := tx.Payload.(DeleteClassPayload)
	if !ok {
		return errors.Errorf("expected commit payload to be DeleteClassPayload, but got %T",
			tx.Payload)
	}

	return m.deleteClassApplyChanges(ctx, pl.ClassName)
}

func (m *Manager) handleUpdateClassCommit(ctx context.Context,
	tx *cluster.Transaction,
) error {
	m.Lock()
	defer m.Unlock()

	pl, ok := tx.Payload.(UpdateClassPayload)
	if !ok {
		return errors.Errorf("expected commit payload to be UpdateClassPayload, but got %T",
			tx.Payload)
	}

	if err := m.parseVectorIndexConfig(ctx, pl.Class); err != nil {
		return err
	}

	if err := m.parseShardingConfig(ctx, pl.Class); err != nil {
		return err
	}

	return m.updateClassApplyChanges(ctx, pl.ClassName, pl.Class, pl.State)
}
