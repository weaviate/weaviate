package database

import (
	"github.com/creativesoftwarefdn/weaviate/models"
)

// Describes the schema that is used in Weaviate.
type Schema struct {
	Actions *models.SemanticSchema
	Things  *models.SemanticSchema
}
