//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package db

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
)

type Migrator struct {
	db *DB
}

func (m *Migrator) AddClass(ctx context.Context, kind kind.Kind, class *models.Class) error {
	idx, err := NewIndex(IndexConfig{
		Kind:      kind,
		ClassName: schema.ClassName(class.Class),
		RootPath:  m.db.config.RootPath,
	})
	if err != nil {
		return errors.Wrap(err, "create index")
	}

	for _, prop := range class.Properties {
		err := idx.addProperty(ctx, prop)
		if err != nil {
			return errors.Wrapf(err, "extend idx '%s' with property", idx.ID())
		}
	}

	m.db.indices[idx.ID()] = idx
	return nil
}

func (m *Migrator) DropClass(ctx context.Context, kind kind.Kind, className string) error {
	return fmt.Errorf("dropping a class not (yet) supported")
}

func (m *Migrator) UpdateClass(ctx context.Context, kind kind.Kind, className string, newClassName *string, newKeywords *models.Keywords) error {
	return fmt.Errorf("updating a class not (yet) supported")
}

func (m *Migrator) AddProperty(ctx context.Context, kind kind.Kind, className string, prop *models.Property) error {
	idx := m.db.GetIndex(kind, schema.ClassName(className))
	if idx == nil {
		return fmt.Errorf("cannot add property to a non-existing index for %s/%s",
			kind.Name(), className)
	}

	return idx.addProperty(ctx, prop)
}

func (m *Migrator) DropProperty(ctx context.Context, kind kind.Kind, className string, propertyName string) error {
	return fmt.Errorf("dropping a property not (yet) supported")
}

func (m *Migrator) UpdateProperty(ctx context.Context, kind kind.Kind, className string, propName string, newName *string, newKeywords *models.Keywords) error {
	return fmt.Errorf("changing a property not (yet) supported")
}

func (m *Migrator) UpdatePropertyAddDataType(ctx context.Context, kind kind.Kind, className string, propName string, newDataType string) error {
	return fmt.Errorf("changing a property not (yet) supported")
}

func NewMigrator(db *DB) *Migrator {
	return &Migrator{db: db}
}
