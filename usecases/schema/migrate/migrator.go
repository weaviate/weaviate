//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

// Package migrate provides a simple composer tool, which implements the
// Migrator interface and can take in any number of migrators which themselves
// have to implement the interface
package migrate

import (
	"context"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
)

// Migrator represents both the input and output interface of the Composer
type Migrator interface {
	AddClass(ctx context.Context, kind kind.Kind, class *models.Class) error
	DropClass(ctx context.Context, kind kind.Kind, className string) error
	UpdateClass(ctx context.Context, kind kind.Kind, className string,
		newClassName *string, newKeywords *models.Keywords) error

	AddProperty(ctx context.Context, kind kind.Kind, className string,
		prop *models.Property) error
	DropProperty(ctx context.Context, kind kind.Kind, className string,
		propertyName string) error
	UpdateProperty(ctx context.Context, kind kind.Kind, className string,
		propName string, newName *string, newKeywords *models.Keywords) error
	UpdatePropertyAddDataType(ctx context.Context, kind kind.Kind, className string, propName string, newDataType string) error
}
