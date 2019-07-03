/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */package migrate

import (
	"context"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
)

// Composer is a simple tool that looks like a single migrator to the outside,
// but can call any number of migrators on the inside
type Composer struct {
	migrators []Migrator
}

// New Composer from any number of Migrators
func New(migrators ...Migrator) *Composer {
	return &Composer{migrators: migrators}
}

// AddClass calls all internal AddClass methods and composes the errors
func (c *Composer) AddClass(ctx context.Context, kind kind.Kind,
	class *models.Class) error {

	ec := newErrorComposer()
	for _, m := range c.migrators {
		ec.Add(m.AddClass(ctx, kind, class))
	}

	return ec.Compose()
}

// DropClass calls all internal DropClass methods and composes the errors
func (c *Composer) DropClass(ctx context.Context, kind kind.Kind,
	class string) error {

	ec := newErrorComposer()
	for _, m := range c.migrators {
		ec.Add(m.DropClass(ctx, kind, class))
	}

	return ec.Compose()
}

// UpdateClass calls all internal UpdateClass methods and composes the errors
func (c *Composer) UpdateClass(ctx context.Context, kind kind.Kind,
	class string, newName *string, newKeywords *models.Keywords) error {

	ec := newErrorComposer()
	for _, m := range c.migrators {
		ec.Add(m.UpdateClass(ctx, kind, class, newName, newKeywords))
	}

	return ec.Compose()
}

// AddProperty calls all internal AddProperty methods and composes the errors
func (c *Composer) AddProperty(ctx context.Context, kind kind.Kind,
	class string, prop *models.Property) error {

	ec := newErrorComposer()
	for _, m := range c.migrators {
		ec.Add(m.AddProperty(ctx, kind, class, prop))
	}

	return ec.Compose()
}

// DropProperty calls all internal DropProperty methods and composes the errors
func (c *Composer) DropProperty(ctx context.Context, kind kind.Kind,
	class string, prop string) error {

	ec := newErrorComposer()
	for _, m := range c.migrators {
		ec.Add(m.DropProperty(ctx, kind, class, prop))
	}

	return ec.Compose()
}

// UpdateProperty calls all internal UpdateProperty methods and composes the errors
func (c *Composer) UpdateProperty(ctx context.Context, kind kind.Kind,
	class string, prop string, newName *string,
	newKeywords *models.Keywords) error {

	ec := newErrorComposer()
	for _, m := range c.migrators {
		ec.Add(m.UpdateProperty(ctx, kind, class, prop, newName, newKeywords))
	}

	return ec.Compose()
}

// UpdatePropertyAddDataType calls all internal UpdatePropertyAddDataType methods and composes the errors
func (c *Composer) UpdatePropertyAddDataType(ctx context.Context, kind kind.Kind,
	class string, prop string, dataType string) error {

	ec := newErrorComposer()
	for _, m := range c.migrators {
		ec.Add(m.UpdatePropertyAddDataType(ctx, kind, class, prop, dataType))
	}

	return ec.Compose()
}
