package schema

import (
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/models"
	log "github.com/sirupsen/logrus"
)

// DataType is a representation of the predicate for queries
type DataType string

const (
	// DataTypeCRef The data type is a cross-reference, it is starting with a capital letter
	DataTypeCRef DataType = "cref"
	// DataTypeString The data type is a value of type string
	DataTypeString DataType = "string"
	// DataTypeInt The data type is a value of type int
	DataTypeInt DataType = "int"
	// DataTypeNumber The data type is a value of type number/float
	DataTypeNumber DataType = "number"
	// DataTypeBoolean The data type is a value of type boolean
	DataTypeBoolean DataType = "boolean"
	// DataTypeDate The data type is a value of type date
	DataTypeDate DataType = "date"
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
