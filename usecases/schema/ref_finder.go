//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package schema

import (
	"sort"

	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	libschema "github.com/weaviate/weaviate/entities/schema"
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
	if schema.Objects != nil {
		classes = append(classes, schema.Objects.Classes...)
	}

	return r.findInClassList(className, classes, schema)
}

func (r *RefFinder) findInClassList(needle libschema.ClassName, classes []*models.Class,
	schema libschema.Schema,
) []filters.Path {
	var out []filters.Path

	for _, class := range classes {
		path, ok := r.hasRefTo(needle, class, schema, 1)
		if !ok {
			continue
		}

		out = append(out, path...)
	}

	return r.sortByPathLen(out)
}

func (r *RefFinder) hasRefTo(needle libschema.ClassName, class *models.Class,
	schema libschema.Schema, depth int,
) ([]filters.Path, bool) {
	var out []filters.Path

	if depth > r.depthLimit {
		return nil, false
	}

	for _, prop := range class.Properties {
		dt, err := schema.FindPropertyDataType(prop.DataType)
		if err != nil {
			// silently ignore, maybe the property was deleted in the meantime
			continue
		}

		if !dt.IsReference() {
			continue
		}

		for _, haystack := range dt.Classes() {
			refs := r.refsPerClass(needle, class, prop.Name, haystack, schema, depth)
			out = append(out, refs...)
		}
	}

	return out, len(out) > 0
}

func (r *RefFinder) refsPerClass(needle libschema.ClassName, class *models.Class,
	propName string, haystack libschema.ClassName, schema libschema.Schema,
	depth int,
) []filters.Path {
	if haystack == needle {
		// direct match
		return []filters.Path{
			{
				Class:    libschema.ClassName(class.Class),
				Property: libschema.PropertyName(propName),
				Child: &filters.Path{
					Class:    needle,
					Property: "id",
				},
			},
		}
	}

	// could still be an indirect (recursive) match
	innerClass := schema.FindClassByName(haystack)
	if innerClass == nil {
		return nil
	}
	paths, ok := r.hasRefTo(needle, innerClass, schema, depth+1)
	if !ok {
		return nil
	}

	var out []filters.Path
	for _, path := range paths {
		out = append(out, filters.Path{
			Class:    libschema.ClassName(class.Class),
			Property: libschema.PropertyName(propName),
			Child:    &path,
		})
	}

	return out
}

func (r *RefFinder) sortByPathLen(in []filters.Path) []filters.Path {
	sort.Slice(in, func(i, j int) bool {
		return len(in[i].Slice()) < len(in[j].Slice())
	})

	return in
}
