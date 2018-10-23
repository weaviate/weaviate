package janusgraph

import (
	"errors"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/schema/kind"
)

// Add a class to the Thing or Action schema, depending on the kind parameter.
func (j *Janusgraph) AddClass(kind kind.Kind, class *models.SemanticSchemaClass) error {
	return errors.New("Not supported")
}

// Drop a class from the schema.
func (j *Janusgraph) DropClass(kind kind.Kind, className string) error {
	return errors.New("Not supported")
}

func (j *Janusgraph) UpdateClass(kind kind.Kind, className string, newClassName *string, newKeywords *models.SemanticSchemaKeywords) error {
	return errors.New("Not supported")
}

func (j *Janusgraph) AddProperty(kind kind.Kind, className string, prop *models.SemanticSchemaClassProperty) error {
	return errors.New("Not supported")
}

func (j *Janusgraph) UpdateProperty(kind kind.Kind, className string, propName string, newName *string, newKeywords *models.SemanticSchemaKeywords) error {
	return errors.New("Not supported")
}

func (j *Janusgraph) DropProperty(kind kind.Kind, className string, propName string) error {
	return errors.New("Not supported")
}
