package schema

import (
	"strings"

	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	libschema "github.com/semi-technologies/weaviate/entities/schema"
)

// RefFinder is a helper that lists classes and their possible paths to to a
// desired target class.
//
// For example if the target class is "car". It might list:
// - Person, drives, Car
// - Person, owns, Car
// - Person, friendsWith, Person, drives, Car
// etc.
//
// It will stop at a preconfigured depth limit, to avoid infinite results, such
// as:
// - Person, friendsWith, Person, friendsWith, Person, ..., drives Car
type RefFinder struct {
	schemaGetter schemaGetterForRefFinder
	depthLimit   int
}

// NewRefFinder with SchemaGetter and depth limit
func NewRefFinder(getter schemaGetterForRefFinder, depthLimit int) *RefFinder {
	return &RefFinder{
		schemaGetter: getter,
		depthLimit:   depthLimit,
	}
}

type schemaGetterForRefFinder interface {
	GetSchemaSkipAuth() libschema.Schema
}

func (r *RefFinder) Find(className libschema.ClassName) []filters.Path {
	schema := r.schemaGetter.GetSchemaSkipAuth()

	var classes []*models.Class
	if schema.Actions != nil {
		classes = append(classes, schema.Actions.Classes...)
	}
	if schema.Things != nil {
		classes = append(classes, schema.Things.Classes...)
	}

	return r.findInClassList(className, classes, schema)
}

func (r *RefFinder) findInClassList(needle libschema.ClassName, classes []*models.Class,
	schema libschema.Schema) []filters.Path {
	var out []filters.Path

	for _, class := range classes {
		path, ok := r.hasRefTo(needle, class, schema)
		if !ok {
			continue
		}

		out = append(out, path...)
	}

	return out
}

func (r *RefFinder) hasRefTo(needle libschema.ClassName, class *models.Class,
	schema libschema.Schema) ([]filters.Path, bool) {
	var out []filters.Path

	for _, prop := range class.Properties {
		dt, err := schema.FindPropertyDataType(prop.DataType)
		if err != nil {
			// silently ignore, maybe the property was deleted in the meantime
		}

		if dt.IsPrimitive() {
			continue
		}

		for _, haystack := range dt.Classes() {
			if haystack == needle {
				out = append(out, filters.Path{
					Class:    libschema.ClassName(class.Class),
					Property: libschema.PropertyName(strings.Title(prop.Name)),
					Child: &filters.Path{
						Class:    needle,
						Property: "uuid",
					},
				})

			}
		}
	}

	return out, len(out) > 0
}
