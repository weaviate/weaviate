package schema

import (
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/models"
	log "github.com/sirupsen/logrus"
)

// Newtype to denote that this string is used as a Class name
type ClassName string

func (c ClassName) String() string {
	return string(c)
}

// Newtype to denote that this string is used as a Property name
type PropertyName string

func (p PropertyName) String() string {
	return string(p)
}

// Describes the schema that is used in Weaviate.
type Schema struct {
	Actions *models.SemanticSchema
	Things  *models.SemanticSchema
}

func Empty() Schema {
	return Schema{
		Actions: &models.SemanticSchema{
			Classes: []*models.SemanticSchemaClass{},
		},
		Things: &models.SemanticSchema{
			Classes: []*models.SemanticSchemaClass{},
		},
	}
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
