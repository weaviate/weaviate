package schema

import "github.com/creativesoftwarefdn/weaviate/entities/schema"

// GetSchema retrieves a locally cached copy of the schema
func (m *Manager) GetSchema() schema.Schema {
	return schema.Schema{
		Actions: m.state.ActionSchema,
		Things:  m.state.ThingSchema,
	}
}
