package janusgraph

import (
	"fmt"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	graphql_local_get "github.com/creativesoftwarefdn/weaviate/graphqlapi/local/get"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/go-openapi/strfmt"
	"strings"
)

func (j *Janusgraph) LocalGetClass(params *graphql_local_get.LocalGetClassParams) (func() interface{}, error) {
	first := 100
	offset := 0

	if params.Pagination != nil {
		first = params.Pagination.First
		offset = params.Pagination.After
	}

	type resolveResult struct {
		results []interface{}
		err     error
	}

	ch := make(chan resolveResult, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				// send error over the channel
				ch <- resolveResult{err: fmt.Errorf("Janusgraph.LocalGetClass paniced: %#v", r)}
			}
			close(ch)
		}()

		results := []interface{}{}

		className := schema.AssertValidClassName(params.ClassName)
		err := j.listClass(params.Kind, &className, first, offset, "", nil, func(uuid strfmt.UUID) {
			var (
				atClass            string
				atContext          string
				foundUUID          strfmt.UUID
				creationTimeUnix   int64
				lastUpdateTimeUnix int64
				properties         models.Schema
				key                *models.SingleRef
			)

			err := j.getClass(params.Kind, uuid, &atClass, &atContext, &foundUUID, &creationTimeUnix, &lastUpdateTimeUnix, &properties, &key)
			if err == nil {
				result := map[string]interface{}{}

				for _, selectProperty := range params.Properties {
					// Primitive properties are trivial; just copy them.
					if selectProperty.IsPrimitive {
						if selectProperty.Name == "uuid" {
							result["uuid"] = interface{}(foundUUID)
						} else {
							propertiesMap := properties.(map[string]interface{})
							result[selectProperty.Name] = propertiesMap[selectProperty.Name]
						}
					} else {
						// For relations we need to do a bit more work.
						propertyName := schema.AssertValidPropertyName(strings.ToLower(selectProperty.Name[0:1]) + selectProperty.Name[1:len(selectProperty.Name)])

						err, property := j.schema.GetProperty(params.Kind, className, propertyName)

						if err != nil {
							panic(fmt.Sprintf("janusgraph.LocalGetClass: could not find property %s in class %s", propertyName, className))
						}

						dataType, err := j.schema.FindPropertyDataType(property.AtDataType)
						if err != nil {
							panic(fmt.Sprintf("janusgraph.LocalGetClass: could find datatype '%#v' for property %s in class", property.AtDataType, propertyName, className))
						}

						cardinality := schema.CardinalityOfProperty(property)
						if cardinality != schema.CardinalityAtMostOne {
							panic("only cardinality of up to most one is currently supported")
						}

						// blurgh
						_ = dataType
						result[selectProperty.Name] = "foobar"
					}
				}

				results = append(results, interface{}(result))
			} else {
				// Silently ignorre the potentially removed things.
			}
		})

		if err != nil {
			ch <- resolveResult{err: fmt.Errorf("Janusgraph.LocalGetClass: %#v", err)}
		} else {
			ch <- resolveResult{results: results}
		}
	}()

	return func() interface{} {
		result := <-ch
		if result.err != nil {
			panic(result.err)
		}
		return result.results
	}, nil
}
