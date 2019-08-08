package search

import (
	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
)

// Result contains some info of a concept (kind), but not all. For
// additional info the ID can be used to retrieve the full concept from the
// connector storage
type Result struct {
	ID        strfmt.UUID
	Kind      kind.Kind
	ClassName string
	Score     float32
	Vector    []float32
	Beacon    string
	Certainty float32
	Schema    models.PropertySchema
}

func (r Result) Thing() *models.Thing {
	schema, ok := r.Schema.(map[string]interface{})
	if ok {
		delete(schema, "uuid")
	}

	t := &models.Thing{
		ID:     r.ID,
		Schema: schema,
	}

	return t
}

func (r Result) Action() *models.Action {
	schema, ok := r.Schema.(map[string]interface{})
	if ok {
		delete(schema, "uuid")
	}

	t := &models.Action{
		ID:     r.ID,
		Schema: schema,
	}

	return t
}
