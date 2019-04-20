package schema

import (
	"context"

	"github.com/creativesoftwarefdn/weaviate/entities/schema/kind"
	"github.com/go-openapi/strfmt"
)

// UpdateMeta for a kind
func (m *Manager) UpdateMeta(ctx context.Context, kind kind.Kind,
	atContext strfmt.URI, maintainer strfmt.Email, name string) error {
	semanticSchema := m.state.SchemaFor(kind)
	semanticSchema.Maintainer = maintainer
	semanticSchema.Name = name

	return m.saveSchema(ctx)
}
