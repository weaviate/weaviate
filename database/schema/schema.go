package schema

import (
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/schema/kind"
	log "github.com/sirupsen/logrus"
)

// Describes the schema that is used in Weaviate.
type Schema struct {
	Actions *models.SemanticSchema
	Things  *models.SemanticSchema
}

// Return one of the semantic schema's
func (s *Schema) SemanticSchemaFor(k kind.Kind) *models.SemanticSchema {
	switch k {
	case kind.THING_KIND:
		return s.Things
	case kind.ACTION_KIND:
		return s.Actions
	default:
		log.Fatalf("No such kind '%s'", k)
		return nil
	}
}
