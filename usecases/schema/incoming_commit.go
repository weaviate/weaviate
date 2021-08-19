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
	default:
		return errors.Errorf("unrecognized commit type %q", tx.Type)
	}
}

func (m *Manager) handleAddClassCommit(ctx context.Context, tx *cluster.Transaction) error {
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

	return m.addClassApplyChanges(ctx, pl.Class, pl.State)
}

func (m *Manager) handleAddPropertyCommit(ctx context.Context,
	tx *cluster.Transaction) error {
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
	tx *cluster.Transaction) error {
	m.Lock()
	defer m.Unlock()

	pl, ok := tx.Payload.(DeleteClassPayload)
	if !ok {
		return errors.Errorf("expected commit payload to be DeleteClassPayload, but got %T",
			tx.Payload)
	}

	return m.deleteClassApplyChanges(ctx, pl.ClassName)
}
