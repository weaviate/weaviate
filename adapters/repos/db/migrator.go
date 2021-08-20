//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package db

import (
	"context"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/sharding"
	"github.com/sirupsen/logrus"
)

type Migrator struct {
	db     *DB
	logger logrus.FieldLogger
}

func (m *Migrator) AddClass(ctx context.Context, class *models.Class,
	shardState *sharding.State) error {
	idx, err := NewIndex(ctx,
		IndexConfig{
			ClassName: schema.ClassName(class.Class),
			RootPath:  m.db.config.RootPath,
		},
		shardState,
		// no backward-compatibility check required, since newly added classes will
		// always have the field set
		class.InvertedIndexConfig,
		class.VectorIndexConfig.(schema.VectorIndexConfig),
		m.db.schemaGetter, m.db, m.logger)
	if err != nil {
		return errors.Wrap(err, "create index")
	}

	err = idx.addUUIDProperty(ctx)
	if err != nil {
		return errors.Wrapf(err, "extend idx '%s' with uuid property", idx.ID())
	}

	for _, prop := range class.Properties {
		if prop.IndexInverted != nil && !*prop.IndexInverted {
			continue
		}

		err := idx.addProperty(ctx, prop)
		if err != nil {
			return errors.Wrapf(err, "extend idx '%s' with property", idx.ID())
		}
	}

	m.db.indices[idx.ID()] = idx
	return nil
}

func (m *Migrator) DropClass(ctx context.Context, className string) error {
	err := m.db.DeleteIndex(schema.ClassName(className))
	if err != nil {
		return errors.Wrapf(err, "delete idx for class '%s'", className)
	}

	return nil
}

func (m *Migrator) UpdateClass(ctx context.Context, className string, newClassName *string) error {
	if newClassName != nil {
		return errors.New("weaviate does not support renaming of classes")
	}

	return nil
}

func (m *Migrator) AddProperty(ctx context.Context, className string, prop *models.Property) error {
	idx := m.db.GetIndex(schema.ClassName(className))
	if idx == nil {
		return errors.Errorf("cannot add property to a non-existing index for %s", className)
	}

	return idx.addProperty(ctx, prop)
}

// DropProperty is ignored, API compliant change
func (m *Migrator) DropProperty(ctx context.Context, className string, propertyName string) error {
	// ignore but don't error
	return nil
}

func (m *Migrator) UpdateProperty(ctx context.Context, className string, propName string, newName *string) error {
	if newName != nil {
		return errors.New("weaviate does not support renaming of properties")
	}

	return nil
}

func NewMigrator(db *DB, logger logrus.FieldLogger) *Migrator {
	return &Migrator{db: db, logger: logger}
}

func (m *Migrator) UpdateVectorIndexConfig(ctx context.Context,
	className string, updated schema.VectorIndexConfig) error {
	idx := m.db.GetIndex(schema.ClassName(className))
	if idx == nil {
		return errors.Errorf("cannot update vector index config of non-existing index for %s", className)
	}

	return idx.updateVectorIndexConfig(ctx, updated)
}

func (m *Migrator) ValidateVectorIndexConfigUpdate(ctx context.Context,
	old, updated schema.VectorIndexConfig) error {
	// hnsw is the only supported vector index type at the moment, so no need
	// to check, we can always use that an hnsw-specific validation should be
	// used for now.
	return hnsw.ValidateUserConfigUpdate(old, updated)
}
