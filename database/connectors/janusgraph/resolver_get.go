/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */
package janusgraph

import (
	"fmt"
	"net/url"
	"runtime/debug"
	"strings"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	graphql_local_common_filters "github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_filters"
	graphql_local_get "github.com/creativesoftwarefdn/weaviate/graphqlapi/local/get"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/network/crossrefs"
	"github.com/go-openapi/strfmt"
)

type resolveResult struct {
	results []interface{}
	err     error
}

// Implement the Local->Get->KIND->CLASS lookup.
func (j *Janusgraph) LocalGetClass(params *graphql_local_get.LocalGetClassParams) (interface{}, error) {
	first := 100
	offset := 0

	if params.Pagination != nil {
		first = params.Pagination.First
		offset = params.Pagination.After
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

		results, err := j.doLocalGetClass(first, offset, params)

		if err != nil {
			ch <- resolveResult{err: fmt.Errorf("Janusgraph.LocalGetClass: %#v", err)}
		} else {
			ch <- resolveResult{results: results}
		}
	}()

	result := <-ch
	if result.err != nil {
		fmt.Printf("Paniced %#v\n", result.err)
		return nil, result.err
	}
	return result.results, nil
}

func (j *Janusgraph) doLocalGetClass(first, offset int, params *graphql_local_get.LocalGetClassParams) ([]interface{}, error) {
	results := []interface{}{}

	className := schema.AssertValidClassName(params.ClassName)
	err := j.listClass(params.Kind, &className, first, offset, "", params.Filters, func(uuid strfmt.UUID) {
		var properties models.Schema
		err := j.getClass(params.Kind, uuid, nil, nil, nil, nil, nil, &properties, nil)
		if err != nil {
			return
		}

		result := j.doLocalGetClassResolveOneClass(params.Kind, className, uuid, params.Properties, properties)

		// nil result? Then we simply could not fetch the class; might be deleted in the mean time?
		if result != nil {
			// fmt.Print("\n\n\n")
			// spew.Dump(params.Filters)
			// fmt.Print("\n\n\n")
			// if matchesFilter(result, params.Filters) {
			results = append(results, result)
			// }
		}
	})

	if err != nil {
		return nil, err
	}

	return results, nil
}

func (j *Janusgraph) doLocalGetClassResolveOneClass(knd kind.Kind, className schema.ClassName,
	foundUUID strfmt.UUID, selectProperties []graphql_local_get.SelectProperty,
	rawSchema models.Schema) map[string]interface{} {
	propertiesMap := rawSchema.(map[string]interface{})

	result := map[string]interface{}{}

	for _, selectProperty := range selectProperties {
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
			// Reference property
			// Yes refer to the original name here, not the normalized name.
			result[selectProperty.Name] = j.doLocalGetClassResolveRefClassProp(knd, className, selectProperty, propertiesMap)
		}
	}

	return result
}

func (j *Janusgraph) doLocalGetClassResolveRefClassProp(knd kind.Kind, className schema.ClassName,
	selectProperty graphql_local_get.SelectProperty, propertiesMap map[string]interface{}) []interface{} {
	propertyName := schema.AssertValidPropertyName(
		strings.ToLower(selectProperty.Name[0:1]) + selectProperty.Name[1:len(selectProperty.Name)])

	err, property := j.schema.GetProperty(knd, className, propertyName)
	if err != nil {
		panic(fmt.Sprintf("janusgraph.LocalGetClass: could not find property %s in class %s", propertyName, className))
	}

	cardinality := schema.CardinalityOfProperty(property)

	// Normalize the refs to a list
	var rawRefs []map[string]interface{}
	if cardinality == schema.CardinalityAtMostOne {
		prop, ok := propertiesMap[string(propertyName)]
		if !ok {
			// the desired property is not present on this instance,
			// simply skip over it
			return []interface{}{}
		}

		propAsMap, ok := prop.(map[string]interface{})
		if !ok {
			panic(fmt.Sprintf(
				"property for %s should be a map, but is %t, the entire propertyMap is \n%#v",
				string(propertyName), propertiesMap[string(propertyName)], propertiesMap))
		}

		rawRefs = append(rawRefs, propAsMap)
	} else {
		for _, rpropAsMap := range propertiesMap[string(propertyName)].([]interface{}) {
			propAsMap := rpropAsMap.(map[string]interface{})
			rawRefs = append(rawRefs, propAsMap)
		}
	}

	// Loop over the raw results
	refResults := []interface{}{}
	for _, rawRef := range rawRefs {
		refType := rawRef["type"].(string)
		refID := strfmt.UUID(rawRef["$cref"].(string))
		refLocation := rawRef["locationUrl"].(string)

		lookupClassKind := classKindFromRefType(refType)
		if lookupClassKind == kind.THING_KIND || lookupClassKind == kind.ACTION_KIND {
			localRef := j.doLocalGetClassResolveLocalRef(selectProperty, lookupClassKind, refID)
			refResults = append(refResults, localRef...)
		}

		if lookupClassKind == kind.NETWORK_THING_KIND || lookupClassKind == kind.NETWORK_ACTION_KIND {
			networkRef := extractNetworkRef(lookupClassKind, refID, refLocation, selectProperty)
			refResults = append(refResults, networkRef...)
		}
	}

	return refResults
}

func (j *Janusgraph) doLocalGetClassResolveLocalRef(selectProperty graphql_local_get.SelectProperty,
	kind kind.Kind, refID strfmt.UUID) []interface{} {
	var refAtClass string
	var refPropertiesSchema models.Schema
	err := j.getClass(kind, refID, &refAtClass, nil, nil, nil, nil, &refPropertiesSchema, nil)
	if err != nil {
		// Skipping broken links for now.
		return []interface{}{}
	}

	// Determine if this is one of the classes that we want to have.
	refClass := schema.AssertValidClassName(refAtClass)
	sc := selectProperty.FindSelectClass(refClass)
	if sc == nil {
		return []interface{}{}
	}

	localRef := j.doLocalGetClassResolveOneClass(kind, refClass, refID, sc.RefProperties, refPropertiesSchema)

	if selectProperty.IncludeTypeName {
		localRef["__typename"] = refAtClass
	}

	return []interface{}{
		graphql_local_get.LocalRef{
			Fields:  localRef,
			AtClass: refAtClass,
		}}
}

func extractNetworkRef(lookupClassKind kind.Kind, refID strfmt.UUID, refLocation string,
	selectProperty graphql_local_get.SelectProperty) []interface{} {
	// from our perspective we are talking about network kinds, but from the
	// peers persepective those are local kinds, so we need to rewrite them:
	var localKind kind.Kind
	switch lookupClassKind {
	case kind.NETWORK_THING_KIND:
		localKind = kind.THING_KIND
	case kind.NETWORK_ACTION_KIND:
		localKind = kind.ACTION_KIND
	}

	locationParsed, err := url.Parse(string(refLocation))
	if err != nil {
		panic(fmt.Errorf("could not parse location (%s) for %s with id '%s': %s", refLocation,
			localKind.TitleizedName(), refID, err))
	}

	// We cannot do an exact check of whether the user specified that
	// they wanted this class as part of their SelectProperties because
	// we would only find out the exact className after resolving this.
	// However, resolving a network ref is a) not the concern of this
	// particular database connector and b) exactly what we want to
	// avoid. The point of checking whether this class is desired is to
	// prevent unnecassary network requests if the user doesn't care
	// about this reference.
	//
	// So instead, we can at least check if the user asked about any
	// classes from this particular peer. If not we can safely determine
	// that this particular network ref is not desired.
	if !selectProperty.HasPeer(locationParsed.Host) {
		return []interface{}{}
	}

	return []interface{}{
		graphql_local_get.NetworkRef{
			NetworkKind: crossrefs.NetworkKind{
				PeerName: locationParsed.Host,
				ID:       refID,
				Kind:     localKind,
			},
		},
	}
}

func matchesFilter(result interface{}, filter *graphql_local_common_filters.LocalFilter) bool {
	if filter == nil {
		return true
	} else {
		return matchesClause(result, filter.Root)
	}
}

func matchesClause(result interface{}, clause *graphql_local_common_filters.Clause) bool {
	if clause == nil {
		return true
	}

	if clause.Operator.OnValue() {
		rawFound := resolvePathInResult(result, clause.On)

		switch clause.Value.Type {
		case schema.DataTypeString:
			found := rawFound.(string)
			expected := clause.Value.Value.(string)
			switch clause.Operator {
			case graphql_local_common_filters.OperatorEqual:
				return found == expected
			case graphql_local_common_filters.OperatorNotEqual:
				return found != expected
			case graphql_local_common_filters.OperatorGreaterThan:
				return found > expected
			case graphql_local_common_filters.OperatorGreaterThanEqual:
				return found >= expected
			case graphql_local_common_filters.OperatorLessThan:
				return found < expected
			case graphql_local_common_filters.OperatorLessThanEqual:
				return found <= expected
			}
		case schema.DataTypeText:
			found := rawFound.(string)
			expected := clause.Value.Value.(string)
			switch clause.Operator {
			case graphql_local_common_filters.OperatorEqual:
				return found == expected
			case graphql_local_common_filters.OperatorNotEqual:
				return found != expected
			case graphql_local_common_filters.OperatorGreaterThan:
				return found > expected
			case graphql_local_common_filters.OperatorGreaterThanEqual:
				return found >= expected
			case graphql_local_common_filters.OperatorLessThan:
				return found < expected
			case graphql_local_common_filters.OperatorLessThanEqual:
				return found <= expected
			}
		case schema.DataTypeInt:
			found := rawFound.(int)
			expected := clause.Value.Value.(int)
			switch clause.Operator {
			case graphql_local_common_filters.OperatorEqual:
				return found == expected
			case graphql_local_common_filters.OperatorNotEqual:
				return found != expected
			case graphql_local_common_filters.OperatorGreaterThan:
				return found > expected
			case graphql_local_common_filters.OperatorGreaterThanEqual:
				return found >= expected
			case graphql_local_common_filters.OperatorLessThan:
				return found < expected
			case graphql_local_common_filters.OperatorLessThanEqual:
				return found <= expected
			}
		case schema.DataTypeNumber:
			found := rawFound.(float64)
			expected := clause.Value.Value.(float64)
			switch clause.Operator {
			case graphql_local_common_filters.OperatorEqual:
				return found == expected
			case graphql_local_common_filters.OperatorNotEqual:
				return found != expected
			case graphql_local_common_filters.OperatorGreaterThan:
				return found > expected
			case graphql_local_common_filters.OperatorGreaterThanEqual:
				return found >= expected
			case graphql_local_common_filters.OperatorLessThan:
				return found < expected
			case graphql_local_common_filters.OperatorLessThanEqual:
				return found <= expected
			}
		case schema.DataTypeBoolean:
			found := rawFound.(bool)
			expected := clause.Value.Value.(bool)
			switch clause.Operator {
			case graphql_local_common_filters.OperatorEqual:
				return found == expected
			case graphql_local_common_filters.OperatorNotEqual:
				return found != expected
			case graphql_local_common_filters.OperatorGreaterThan:
				panic("not supported")
			case graphql_local_common_filters.OperatorGreaterThanEqual:
				panic("not supported")
			case graphql_local_common_filters.OperatorLessThan:
				panic("not supported")
			case graphql_local_common_filters.OperatorLessThanEqual:
				panic("not supported")
			}
		default:
			panic("not supported")
		}
	} else {
		switch clause.Operator {
		case graphql_local_common_filters.OperatorAnd:
			for _, operand := range clause.Operands {
				if !matchesClause(result, &operand) {
					return false
				}
			}
			return true
		case graphql_local_common_filters.OperatorOr:
			for _, operand := range clause.Operands {
				if matchesClause(result, &operand) {
					return true
				}
			}
			return false
		case graphql_local_common_filters.OperatorNot:
			for _, operand := range clause.Operands {
				if matchesClause(result, &operand) {
					return false
				}
			}
			return true
		default:
			panic("Unknown operator")
		}
	}

	panic("should be unreachable")
}

func resolvePathInResult(result interface{}, path *graphql_local_common_filters.Path) interface{} {
	var resultAsMap map[string]interface{}
	switch v := result.(type) {
	case map[string]interface{}:
		resultAsMap = v
	case []interface{}:
		if len(v) != 1 {
			panic("only single refs supported for now")
		} else {
			resultAsMap = v[0].(map[string]interface{})
		}
	}
	if path.Child == nil {
		return resultAsMap[path.Property.String()]
	} else {
		return resolvePathInResult(resultAsMap[strings.Title(path.Property.String())], path.Child)
	}
}

func classKindFromRefType(refType string) kind.Kind {
	var result kind.Kind
	switch refType {
	case "Thing":
		result = kind.THING_KIND
	case "Action":
		result = kind.ACTION_KIND
	case "NetworkThing":
		result = kind.NETWORK_THING_KIND
	case "NetworkAction":
		result = kind.NETWORK_ACTION_KIND
	default:
		panic(fmt.Sprintf("unsupported kind in reference: %s", refType))
	}
	return result
}
