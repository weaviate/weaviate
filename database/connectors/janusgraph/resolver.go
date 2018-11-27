package janusgraph

import (
	"fmt"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	graphql_local_get "github.com/creativesoftwarefdn/weaviate/graphqlapi/local/get"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/go-openapi/strfmt"
	"runtime/debug"
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
				ch <- resolveResult{err: fmt.Errorf("Janusgraph.LocalGetClass paniced: %#v\n%s", r, string(debug.Stack()))}
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
				propertiesMap := properties.(map[string]interface{})

				result := map[string]interface{}{}

				for _, selectProperty := range params.Properties {
					if selectProperty.Name == "uuid" {
						result["uuid"] = interface{}(foundUUID)
						continue
					}

					// Primitive properties are trivial; just copy them.
					if selectProperty.IsPrimitive {
						_, isPresent := propertiesMap[selectProperty.Name]
						if !isPresent {
							continue
						}
						result[selectProperty.Name] = propertiesMap[selectProperty.Name]
					} else {
						// For relations we need to do a bit more work.
						propertyName := schema.AssertValidPropertyName(strings.ToLower(selectProperty.Name[0:1]) + selectProperty.Name[1:len(selectProperty.Name)])

						err, property := j.schema.GetProperty(params.Kind, className, propertyName)

						if err != nil {
							panic(fmt.Sprintf("janusgraph.LocalGetClass: could not find property %s in class %s", propertyName, className))
						}

						cardinality := schema.CardinalityOfProperty(property)

						// Normalize the refs to a list
						var rawRefs []map[string]interface{}
						if cardinality == schema.CardinalityAtMostOne {
							propAsMap := propertiesMap[string(propertyName)].(map[string]interface{})
							rawRefs = append(rawRefs, propAsMap)
						} else {
							for _, rpropAsMap := range propertiesMap[string(propertyName)].([]interface{}) {
								propAsMap := rpropAsMap.(map[string]interface{})
								rawRefs = append(rawRefs, propAsMap)
							}
						}

						refResults := []interface{}{}

						// Loop over the raw results
						for _, rawRef := range rawRefs {
							refType := rawRef["type"].(string)
							refId := strfmt.UUID(rawRef["$cref"].(string))

							var refAtClass string
							var refPropertiesSchema models.Schema

							var lookupClassKind kind.Kind
							switch refType {
							case "Thing":
								lookupClassKind = kind.THING_KIND
							case "Action":
								lookupClassKind = kind.ACTION_KIND
							default:
								panic("unsupported kind in reference")
							}

							err := j.getClass(lookupClassKind, refId, &refAtClass, nil, nil, nil, nil, &refPropertiesSchema, nil)
							if err != nil {
								// TODO; skipping broken links for now.
								continue
							}
							refProperties := refPropertiesSchema.(map[string]interface{})

							// Determine if this is one of the classes that we want to have.
							if sc := selectProperty.FindSelectClass(schema.AssertValidClassName(refAtClass)); sc != nil {
								refResult := map[string]interface{}{}

								if selectProperty.IncludeTypeName {
									refResult["__typename"] = refAtClass
								}

								// todo: loop over sub props.
								// TODO recurse
								for _, prop := range sc.RefProperties {
									if prop.IsPrimitive {
										refResult[prop.Name] = refProperties[prop.Name]
									}
								}

								refResults = append(refResults, refResult)
							}
						}

						// Yes refer to the original name here, not the normalized name.
						result[selectProperty.Name] = refResults
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
