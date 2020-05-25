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

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
)

type Migrator struct {
	db *DB
}

func (m *Migrator) AddClass(ctx context.Context, kind kind.Kind, class *models.Class) error {
	panic("not implemented") // TODO: Implement
}

func (m *Migrator) DropClass(ctx context.Context, kind kind.Kind, className string) error {
	panic("not implemented") // TODO: Implement
}

func (m *Migrator) UpdateClass(ctx context.Context, kind kind.Kind, className string, newClassName *string, newKeywords *models.Keywords) error {
	panic("not implemented") // TODO: Implement
}

func (m *Migrator) AddProperty(ctx context.Context, kind kind.Kind, className string, prop *models.Property) error {
	panic("not implemented") // TODO: Implement
}

func (m *Migrator) DropProperty(ctx context.Context, kind kind.Kind, className string, propertyName string) error {
	panic("not implemented") // TODO: Implement
}

func (m *Migrator) UpdateProperty(ctx context.Context, kind kind.Kind, className string, propName string, newName *string, newKeywords *models.Keywords) error {
	panic("not implemented") // TODO: Implement
}

func (m *Migrator) UpdatePropertyAddDataType(ctx context.Context, kind kind.Kind, className string, propName string, newDataType string) error {
	panic("not implemented") // TODO: Implement
}

func NewMigrator(db *DB) *Migrator {
	return &Migrator{db: db}
}
