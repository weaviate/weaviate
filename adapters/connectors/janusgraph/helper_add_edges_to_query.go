/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */
package janusgraph

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/semi-technologies/weaviate/adapters/connectors/janusgraph/gremlin"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
)

// map properties in thing.Schema according to the mapping.
type edge struct {
	PropertyName string
	*crossref.Ref
}

type edgeFromRefProp struct {
	localEdges   []edge
	networkEdges []edge
	edgesToDrop  []string
}

func (j *Janusgraph) addEdgesToQuery(ctx context.Context, q *gremlin.Query, k kind.Kind, className schema.ClassName,
	rawProperties interface{}, janusSourceClassLabel string) (*gremlin.Query, error) {

	var localEdges []edge
	var networkEdges []edge
	var dropTheseEdgeTypes []string

	properties, ok := rawProperties.(map[string]interface{})
	if !ok {
		// nothing to do because we don't have any
		// (useable) properties
		return q, nil
	}

	for propName, value := range properties {
		sanitizedPropertyName := schema.AssertValidPropertyName(propName)
		err, property := j.schema.GetProperty(k, className, sanitizedPropertyName)
		if err != nil {
			return q, err
		}

		janusPropertyName := string(
			j.state.MustGetMappedPropertyName(className, sanitizedPropertyName))
		propType, err := j.schema.FindPropertyDataType(property.DataType)
		if err != nil {
			return q, err
		}

		if propType.IsPrimitive() {
			q, err = addPrimitivePropToQuery(q, propType, value,
				janusPropertyName, sanitizedPropertyName)
			if err != nil {
				return q, err
			}
		} else {
			result, err := j.edgesFromReferenceProp(ctx, property, value, propType, janusPropertyName, sanitizedPropertyName)
			if err != nil {
				return q, err
			}

			localEdges = append(localEdges, result.localEdges...)
			networkEdges = append(networkEdges, result.networkEdges...)
			dropTheseEdgeTypes = append(dropTheseEdgeTypes, result.edgesToDrop...)
		}
	}

	// Now drop all edges of the type we are touching
	for _, edgeLabel := range dropTheseEdgeTypes {
		q = q.Optional(gremlin.Current().OutEWithLabel(edgeLabel).HasString(PROP_REF_ID, edgeLabel).Drop())
	}

	// (Re-)Add edges to all local refs
	for _, edge := range localEdges {
		q = q.AddE(edge.PropertyName).
			FromRef(janusSourceClassLabel).
			ToQuery(gremlin.G.V().HasString(PROP_UUID, string(edge.TargetID))).
			StringProperty(PROP_REF_ID, edge.PropertyName).
			StringProperty(PROP_REF_EDGE_CREF, string(edge.TargetID)).
			StringProperty(PROP_REF_EDGE_TYPE, edge.Kind.Name()).
			StringProperty(PROP_REF_EDGE_LOCATION, edge.PeerName)
	}

	// (Re-)Add edges to all network refs
	for _, edge := range networkEdges {
		q = q.AddE(edge.PropertyName).
			FromRef(janusSourceClassLabel).
			ToQuery(
				gremlin.G.V().HasString(PROP_UUID, string(edge.TargetID)).
					Fold().
					Coalesce(gremlin.RawQuery(
						fmt.Sprintf("unfold(), addV().property(\"uuid\", \"%s\")", string(edge.TargetID)),
					)),
			).
			StringProperty(PROP_REF_ID, edge.PropertyName).
			StringProperty(PROP_REF_EDGE_CREF, string(edge.TargetID)).
			StringProperty(PROP_REF_EDGE_TYPE, edge.Kind.Name()).
			StringProperty(PROP_REF_EDGE_LOCATION, edge.PeerName)
	}

	return q, nil
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
		case json.Number:
			asInt, err := t.Int64()
			if err != nil {
				return q, fmt.Errorf("Illegal json.Number value for property %s, could not be converted to int64: %s", sanitizedPropertyName, err)
			}

			q = q.Int64Property(janusPropertyName, asInt)
		default:
			return q, fmt.Errorf("Illegal primitive value for property %s, value is %#v", sanitizedPropertyName, t)
		}
	case schema.DataTypeString:
		switch t := value.(type) {
		case string:
			q = q.StringProperty(janusPropertyName, t)
		default:
			return q, fmt.Errorf("Illegal primitive value for property %s, value is %#v", sanitizedPropertyName, t)
		}
	case schema.DataTypeText:
		switch t := value.(type) {
		case string:
			q = q.StringProperty(janusPropertyName, t)
		default:
			return q, fmt.Errorf("Illegal primitive value for property %s, value is %#v", sanitizedPropertyName, t)
		}
	case schema.DataTypeBoolean:
		switch t := value.(type) {
		case bool:
			q = q.BoolProperty(janusPropertyName, t)
		default:
			return q, fmt.Errorf("Illegal primitive value for property %s, value is %#v", sanitizedPropertyName, t)
		}
	case schema.DataTypeNumber:
		switch t := value.(type) {
		case float32:
			q = q.Float64Property(janusPropertyName, float64(t))
		case float64:
			q = q.Float64Property(janusPropertyName, t)
		case json.Number:
			asFloat, err := t.Float64()
			if err != nil {
				return q, fmt.Errorf("Illegal json.Number value for property %s, could not be converted to float64: %s", sanitizedPropertyName, err)
			}

			q = q.Float64Property(janusPropertyName, asFloat)
		default:
			return q, fmt.Errorf("Illegal primitive value for property %s, value is %#v", sanitizedPropertyName, t)
		}
	case schema.DataTypeDate:
		switch t := value.(type) {
		case time.Time:
			q = q.StringProperty(janusPropertyName, t.Format(time.RFC3339))
		default:
			return q, fmt.Errorf("Illegal primitive value for property %s, value is %#v", sanitizedPropertyName, t)
		}
	case schema.DataTypeGeoCoordinates:
		point, ok := value.(*models.GeoCoordinates)
		if !ok {
			return q, fmt.Errorf("Illegal value for property '%s' of type geoCoordinates, value is %T", sanitizedPropertyName, value)
		}

		q = q.GeoshapePointProperty(janusPropertyName, point.Latitude, point.Longitude)
	default:
		panic(fmt.Sprintf("Unknown primitive datatype %s", propType.AsPrimitive()))
	}

	return q, nil
}

func (j *Janusgraph) edgesFromReferenceProp(ctx context.Context, property *models.SemanticSchemaClassProperty,
	value interface{}, propType schema.PropertyDataType, janusPropertyName string, sanitizedPropertyName schema.PropertyName) (edgeFromRefProp, error) {
	result := edgeFromRefProp{}

	switch schema.CardinalityOfProperty(property) {
	case schema.CardinalityAtMostOne:
		return j.singleRef(ctx, value, propType, janusPropertyName, sanitizedPropertyName)
	case schema.CardinalityMany:
		return j.multipleRefs(ctx, value, propType, janusPropertyName, sanitizedPropertyName)
	default:
		return result, fmt.Errorf("Unexpected cardinality %v",
			schema.CardinalityOfProperty(property))
	}
}

func (j *Janusgraph) singleRef(ctx context.Context, value interface{}, propType schema.PropertyDataType,
	janusPropertyName string, sanitizedPropertyName schema.PropertyName) (edgeFromRefProp, error) {
	result := edgeFromRefProp{}
	switch ref := value.(type) {
	case *models.SingleRef:
		parsedRef, err := crossref.ParseSingleRef(ref)
		if err != nil {
			return result, err
		}

		if parsedRef.Local {
			return j.singleLocalRef(ctx, parsedRef, propType, janusPropertyName, sanitizedPropertyName)
		}
		return j.singleNetworkRef(ctx, parsedRef, janusPropertyName)

	default:
		return result, fmt.Errorf("Illegal value for property %s", sanitizedPropertyName)
	}
}

func (j *Janusgraph) singleNetworkRef(ctx context.Context, ref *crossref.Ref,
	janusPropertyName string) (edgeFromRefProp, error) {
	result := edgeFromRefProp{}
	// We can't do any business-validation in here (such as does this
	// NetworkThing/Action really exist on that particular network instance?), as
	// we are in a (local) database connector.  Network validations are not our
	// concern. We must trust that a previous layer has verified the correctness.

	result.networkEdges = []edge{{
		PropertyName: janusPropertyName,
		Ref:          ref,
	}}
	return result, nil
}

func (j *Janusgraph) singleLocalRef(ctx context.Context, ref *crossref.Ref, propType schema.PropertyDataType,
	janusPropertyName string, sanitizedPropertyName schema.PropertyName) (edgeFromRefProp, error) {
	var refClassName schema.ClassName
	result := edgeFromRefProp{}

	switch ref.Kind {
	case kind.Action:
		var singleRefValue models.Action
		err := j.GetAction(ctx, ref.TargetID, &singleRefValue)
		if err != nil {
			return result, fmt.Errorf("Illegal value for property %s; could not resolve action with UUID: %v", ref.TargetID.String(), err)
		}
		refClassName = schema.AssertValidClassName(singleRefValue.Class)
	case kind.Thing:
		var singleRefValue models.Thing
		err := j.GetThing(ctx, ref.TargetID, &singleRefValue)
		if err != nil {
			return result, fmt.Errorf("Illegal value for property %s; could not resolve thing with UUID: %v", ref.TargetID.String(), err)
		}
		refClassName = schema.AssertValidClassName(singleRefValue.Class)
	}

	// Verify the cross reference
	if !propType.ContainsClass(refClassName) {
		return result, fmt.Errorf("Illegal value for property %s; cannot point to %s", sanitizedPropertyName, ref.Kind.Name())
	}
	result.localEdges = []edge{{
		PropertyName: janusPropertyName,
		Ref:          ref,
	}}

	return result, nil
}

func (j *Janusgraph) multipleRefs(ctx context.Context, value interface{}, propType schema.PropertyDataType,
	janusPropertyName string, sanitizedPropertyName schema.PropertyName) (edgeFromRefProp, error) {
	result := edgeFromRefProp{}
	result.edgesToDrop = []string{janusPropertyName}
	switch t := value.(type) {
	case models.MultipleRef, *models.MultipleRef:
		refs := derefMultipleRefsIfNeeded(t)
		for _, ref := range refs {
			singleRef, err := j.singleRef(ctx, ref, propType, janusPropertyName, sanitizedPropertyName)
			if err != nil {
				return result, err
			}
			result.localEdges = append(result.localEdges, singleRef.localEdges...)
			result.networkEdges = append(result.networkEdges, singleRef.networkEdges...)
		}
		return result, nil
	case []interface{}:
		for _, raw := range t {
			ref, ok := raw.(*models.SingleRef)
			if !ok {
				return result, fmt.Errorf(
					"illegal value for property %s: expected a list of single refs, but current item is %#v",
					sanitizedPropertyName, raw)
			}
			singleRef, err := j.singleRef(ctx, ref, propType, janusPropertyName, sanitizedPropertyName)
			if err != nil {
				return result, err
			}
			result.localEdges = append(result.localEdges, singleRef.localEdges...)
			result.networkEdges = append(result.networkEdges, singleRef.networkEdges...)
		}
		return result, nil
	default:
		return result, fmt.Errorf("illegal value for property %s, expected *models.MultipleRef, but got %#v",
			sanitizedPropertyName, value)
	}
}

func derefMultipleRefsIfNeeded(t interface{}) models.MultipleRef {
	switch typed := t.(type) {
	case models.MultipleRef:
		// during a patch we don't get a pointer type
		return typed
	case *models.MultipleRef:
		// during a put we get a pointer type
		return *typed
	default:
		// impossible to reach since it's only used after previous type assertion
		panic("neither *models.MultipleRef nor models.MultipleRef received")
	}
}
