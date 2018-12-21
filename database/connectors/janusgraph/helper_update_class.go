package janusgraph

import (
	"fmt"
	"time"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/gremlin"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/go-openapi/strfmt"
)

func (j *Janusgraph) updateClass(k kind.Kind, className schema.ClassName, UUID strfmt.UUID, atContext string, lastUpdateTimeUnix int64, rawProperties interface{}) error {
	vertexLabel := j.state.getMappedClassName(className)

	q := gremlin.G.V().
		HasString(PROP_KIND, k.Name()).
		HasString(PROP_UUID, UUID.String()).
		As("class").
		StringProperty(PROP_CLASS_ID, string(vertexLabel)).
		StringProperty(PROP_AT_CONTEXT, atContext).
		Int64Property(PROP_LAST_UPDATE_TIME_UNIX, lastUpdateTimeUnix)

	// map properties in thing.Schema according to the mapping.
	type expectedEdge struct {
		PropertyName string
		Type         string
		Reference    string
		Location     string
	}

	var expectedEdges []expectedEdge
	var dropTheseEdgeTypes []string

	properties, schema_ok := rawProperties.(map[string]interface{})
	if schema_ok {
		for propName, value := range properties {
			sanitizedPropertyName := schema.AssertValidPropertyName(propName)
			err, property := j.schema.GetProperty(k, className, sanitizedPropertyName)
			if err != nil {
				return err
			}

			janusPropertyName := string(
				j.state.getMappedPropertyName(className, sanitizedPropertyName))
			propType, err := j.schema.FindPropertyDataType(property.AtDataType)
			if err != nil {
				return err
			}

			if propType.IsPrimitive() {
				q, err = addPrimitivePropToQuery(q, propType, value,
					janusPropertyName, sanitizedPropertyName)
				if err != nil {
					return err
				}
			} else {
				switch schema.CardinalityOfProperty(property) {
				case schema.CardinalityAtMostOne:
					switch t := value.(type) {
					case *models.SingleRef:
						var refClassName schema.ClassName
						switch t.Type {
						case "NetworkThing", "NetworkAction":
							refClassName = "something"
							// simply jump over this entire property for now
							// so import works again
							continue

						case "Action":
							var singleRefValue models.ActionGetResponse
							err = j.GetAction(nil, t.NrDollarCref, &singleRefValue)
							if err != nil {
								return fmt.Errorf("Illegal value for property %s; could not resolve action with UUID: %v", t.NrDollarCref.String(), err)
							}
							refClassName = schema.AssertValidClassName(singleRefValue.AtClass)
						case "Thing":
							var singleRefValue models.ThingGetResponse
							err = j.GetThing(nil, t.NrDollarCref, &singleRefValue)
							if err != nil {
								return fmt.Errorf("Illegal value for property %s; could not resolve thing with UUID: %v", t.NrDollarCref.String(), err)
							}
							refClassName = schema.AssertValidClassName(singleRefValue.AtClass)
						default:
							return fmt.Errorf("illegal value for property %s; only Thing or Action supported", t.Type)
						}

						// Verify the cross reference
						if !propType.ContainsClass(refClassName) {
							return fmt.Errorf("Illegal value for property %s; cannot point to %s", sanitizedPropertyName, t.Type)
						}
						expectedEdges = append(expectedEdges, expectedEdge{
							PropertyName: janusPropertyName,
							Reference:    t.NrDollarCref.String(),
							Type:         t.Type,
							Location:     *t.LocationURL,
						})
					default:
						return fmt.Errorf("Illegal value for property %s", sanitizedPropertyName)
					}
				case schema.CardinalityMany:
					dropTheseEdgeTypes = append(dropTheseEdgeTypes, janusPropertyName)
					switch t := value.(type) {
					case *models.MultipleRef:
						for _, ref := range *t {
							expectedEdges = append(expectedEdges, expectedEdge{
								PropertyName: janusPropertyName,
								Reference:    ref.NrDollarCref.String(),
								Type:         ref.Type,
								Location:     *ref.LocationURL,
							})
						}
					case []interface{}:
						for _, ref_ := range t {
							ref, ok := ref_.(*models.SingleRef)
							if !ok {
								return fmt.Errorf("Illegal value for property %s", sanitizedPropertyName)
							}
							expectedEdges = append(expectedEdges, expectedEdge{
								PropertyName: janusPropertyName,
								Reference:    ref.NrDollarCref.String(),
								Type:         ref.Type,
								Location:     *ref.LocationURL,
							})
						}
					default:
						return fmt.Errorf("Illegal value for property %s", sanitizedPropertyName)
					}
				default:
					panic(fmt.Sprintf("Unexpected cardinality %v", schema.CardinalityOfProperty(property)))
				}
			}
		}
	}

	// Now drop all edges of the type we are touching
	for _, edgeLabel := range dropTheseEdgeTypes {
		q = q.Optional(gremlin.Current().OutEWithLabel(edgeLabel).HasString(PROP_REF_ID, edgeLabel).Drop())
	}

	// Add edges to all referened things.
	for _, edge := range expectedEdges {
		q = q.AddE(edge.PropertyName).
			FromRef("class").
			ToQuery(gremlin.G.V().HasString(PROP_UUID, edge.Reference)).
			StringProperty(PROP_REF_ID, edge.PropertyName).
			StringProperty(PROP_REF_EDGE_CREF, edge.Reference).
			StringProperty(PROP_REF_EDGE_TYPE, edge.Type).
			StringProperty(PROP_REF_EDGE_LOCATION, edge.Location)
	}

	_, err := j.client.Execute(q)

	return err
}

func addPrimitivePropToQuery(q *gremlin.Query, propType schema.PropertyDataType,
	value interface{}, janusPropertyName string, sanitizedPropertyName schema.PropertyName,
) (*gremlin.Query, error) {
	switch propType.AsPrimitive() {
	case schema.DataTypeInt:
		switch t := value.(type) {
		case int:
			q = q.Int64Property(janusPropertyName, int64(t))
		case int8:
			q = q.Int64Property(janusPropertyName, int64(t))
		case int16:
			q = q.Int64Property(janusPropertyName, int64(t))
		case int32:
			q = q.Int64Property(janusPropertyName, int64(t))
		case int64:
			q = q.Int64Property(janusPropertyName, t)
		case uint:
			q = q.Int64Property(janusPropertyName, int64(t))
		case uint8:
			q = q.Int64Property(janusPropertyName, int64(t))
		case uint16:
			q = q.Int64Property(janusPropertyName, int64(t))
		case uint32:
			q = q.Int64Property(janusPropertyName, int64(t))
		case uint64:
			q = q.Int64Property(janusPropertyName, int64(t))
		case float32:
			q = q.Int64Property(janusPropertyName, int64(t))
		case float64:
			q = q.Int64Property(janusPropertyName, int64(t))
		default:
			return q, fmt.Errorf("Illegal value for property %s", sanitizedPropertyName)
		}
	case schema.DataTypeString:
		switch t := value.(type) {
		case string:
			q = q.StringProperty(janusPropertyName, t)
		default:
			return q, fmt.Errorf("Illegal value for property %s", sanitizedPropertyName)
		}
	case schema.DataTypeText:
		switch t := value.(type) {
		case string:
			q = q.StringProperty(janusPropertyName, t)
		default:
			return q, fmt.Errorf("Illegal value for property %s", sanitizedPropertyName)
		}
	case schema.DataTypeBoolean:
		switch t := value.(type) {
		case bool:
			q = q.BoolProperty(janusPropertyName, t)
		default:
			return q, fmt.Errorf("Illegal value for property %s", sanitizedPropertyName)
		}
	case schema.DataTypeNumber:
		switch t := value.(type) {
		case float32:
			q = q.Float64Property(janusPropertyName, float64(t))
		case float64:
			q = q.Float64Property(janusPropertyName, t)
		default:
			return q, fmt.Errorf("Illegal value for property %s", sanitizedPropertyName)
		}
	case schema.DataTypeDate:
		switch t := value.(type) {
		case time.Time:
			q = q.StringProperty(janusPropertyName, t.Format(time.RFC3339))
		default:
			return q, fmt.Errorf("Illegal value for property %s", sanitizedPropertyName)
		}
	}
	panic(fmt.Sprintf("Unkown primitive datatype %s", propType.AsPrimitive()))
}
