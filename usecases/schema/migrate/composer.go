package migrate

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
func (c *Composer) AddClass(ctx context.Context, kind kind.Kind, class *models.SemanticSchemaClass) error {

	for _, m := range c.migrators {
		m.AddClass(ctx, kind, class)
	}

	return nil
}
