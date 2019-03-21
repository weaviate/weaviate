/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */
package schema

// This file contains the logic to build an in-memory contextionary from the actions & things classes and properties.

import (
	"fmt"
	"strings"

	"github.com/fatih/camelcase"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/models"

	libcontextionary "github.com/creativesoftwarefdn/weaviate/contextionary"
)

func BuildInMemoryContextionaryFromSchema(schema schema.Schema, context *libcontextionary.Contextionary) (*libcontextionary.Contextionary, error) {
	in_memory_builder := libcontextionary.InMemoryBuilder((*context).GetVectorLength())

	err := add_names_from_schema_properties(context, in_memory_builder, "THING", schema.SemanticSchemaFor(kind.THING_KIND))
	if err != nil {
		return nil, err
	}

	err = add_names_from_schema_properties(context, in_memory_builder, "ACTION", schema.SemanticSchemaFor(kind.ACTION_KIND))
	if err != nil {
		return nil, err
	}

	in_memory_contextionary := in_memory_builder.Build(10)
	x := libcontextionary.Contextionary(in_memory_contextionary)
	return &x, nil
}

// This function adds words in the form of $THING[Blurp]
func add_names_from_schema_properties(context *libcontextionary.Contextionary, in_memory_builder *libcontextionary.MemoryIndexBuilder, kind string, schema *models.SemanticSchema) error {
	for _, class := range schema.Classes {
		class_centroid_name := fmt.Sprintf("$%v[%v]", kind, class.Class)

		// Are there keywords? If so, use those
		if len(class.Keywords) > 0 {
			var vectors []libcontextionary.Vector = make([]libcontextionary.Vector, 0)
			var weights []float32 = make([]float32, 0)

			for _, keyword := range class.Keywords {
				word := strings.ToLower(keyword.Keyword)
				// Lookup vector for the keyword.
				idx := (*context).WordToItemIndex(word)
				if idx.IsPresent() {
					vector, err := (*context).GetVectorForItemIndex(idx)

					if err != nil {
						return fmt.Errorf("Could not fetch vector for a found index. Data corruption?")
					} else {
						vectors = append(vectors, *vector)
						weights = append(weights, keyword.Weight)
					}
				} else {
					return fmt.Errorf("Could not find keyword '%v' for class '%v' in the contextionary", word, class.Class)
				}
			}

			centroid, err := libcontextionary.ComputeWeightedCentroid(vectors, weights)
			if err != nil {
				return fmt.Errorf("Could not compute centroid")
			} else {
				in_memory_builder.AddWord(class_centroid_name, *centroid)
			}
		} else {
			// No keywords specified; split name on camel case, and add each word part to a equally weighted word vector.
			camel_parts := camelcase.Split(class.Class)
			var vectors []libcontextionary.Vector = make([]libcontextionary.Vector, 0)
			for _, part := range camel_parts {
				part = strings.ToLower(part)
				// Lookup vector for the keyword.
				idx := (*context).WordToItemIndex(part)
				if idx.IsPresent() {
					vector, err := (*context).GetVectorForItemIndex(idx)

					if err != nil {
						return fmt.Errorf("Could not fetch vector for a found index. Data corruption?")
					} else {
						vectors = append(vectors, *vector)
					}
				} else {
					return fmt.Errorf("Could not find camel cased name part '%v' for class '%v' in the contextionary", part, class.Class)
				}
			}

			centroid, err := libcontextionary.ComputeCentroid(vectors)
			if err != nil {
				return fmt.Errorf("Could not compute centroid")
			} else {
				in_memory_builder.AddWord(class_centroid_name, *centroid)
			}
		}

		// NOW FOR THE PROPERTIES;
		// basically the same code as above.
		for _, property := range class.Properties {
			property_centroid_name := fmt.Sprintf("%v[%v]", class_centroid_name, property.Name)

			// Are there keywords? If so, use those
			if len(property.Keywords) > 0 {
				var vectors []libcontextionary.Vector = make([]libcontextionary.Vector, 0)
				var weights []float32 = make([]float32, 0)

				for _, keyword := range property.Keywords {
					word := strings.ToLower(keyword.Keyword)
					// Lookup vector for the keyword.
					idx := (*context).WordToItemIndex(word)
					if idx.IsPresent() {
						vector, err := (*context).GetVectorForItemIndex(idx)

						if err != nil {
							return fmt.Errorf("Could not fetch vector for a found index. Data corruption?")
						} else {
							vectors = append(vectors, *vector)
							weights = append(weights, keyword.Weight)
						}
					} else {
						return fmt.Errorf("Could not find keyword '%v' for class '%v' in the contextionary, please choose another keyword", word, class.Class)
					}
				}

				centroid, err := libcontextionary.ComputeWeightedCentroid(vectors, weights)
				if err != nil {
					return fmt.Errorf("Could not compute centroid")
				} else {
					in_memory_builder.AddWord(property_centroid_name, *centroid)
				}
			} else {
				// No keywords specified; split name on camel case, and add each word part to a equally weighted word vector.
				camel_parts := camelcase.Split(property.Name)
				var vectors []libcontextionary.Vector = make([]libcontextionary.Vector, 0)
				for _, part := range camel_parts {
					part = strings.ToLower(part)
					// Lookup vector for the keyword.
					idx := (*context).WordToItemIndex(part)
					if idx.IsPresent() {
						vector, err := (*context).GetVectorForItemIndex(idx)

						if err != nil {
							return fmt.Errorf("Could not fetch vector for a found index. Data corruption?")
						} else {
							vectors = append(vectors, *vector)
						}
					} else {
						return fmt.Errorf("Could not find camel cased part of name '%v' for property %v in class '%v' in the contextionary, consider adding some keywords instead.", part, property.Name, class.Class)
					}
				}

				centroid, err := libcontextionary.ComputeCentroid(vectors)
				if err != nil {
					return fmt.Errorf("Could not compute centroid")
				} else {
					in_memory_builder.AddWord(property_centroid_name, *centroid)
				}
			}
		}
	}

	return nil
}
