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
package janusgraph

import (
	"fmt"
	"runtime/debug"
	"strings"

	jget "github.com/creativesoftwarefdn/weaviate/database/connectors/janusgraph/get"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/crossref"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	graphql_local_common_filters "github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_filters"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/get"
	"github.com/creativesoftwarefdn/weaviate/gremlin"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/network/crossrefs"
	"github.com/davecgh/go-spew/spew"
	"github.com/go-openapi/strfmt"
)

type resolveResult struct {
	results []interface{}
	err     error
}

// Implement the Local->Get->KIND->CLASS lookup.
func (j *Janusgraph) LocalGetClass(params *get.Params) (interface{}, error) {
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

func (j *Janusgraph) doLocalGetClass(first, offset int, params *get.Params) ([]interface{}, error) {
	// new part

	q, err := jget.NewQuery(*params, &j.state, &j.schema).String()
	if err != nil {
		return nil, fmt.Errorf("could not build query: %s", err)
	}

	res, err := j.client.Execute(gremlin.New().Raw(q))
	if err != nil {
		return nil, fmt.Errorf("could not execute new query: %s", err)
	}

	fmt.Printf("\n\n\n%s\n\n\n\n", spew.Sdump(res.Data))

	// old part

	className := schema.AssertValidClassName(params.ClassName)
	classes, err := j.getClasses(params.Kind, &className, first, offset, params.Filters)
	if err != nil {
		return nil, fmt.Errorf("the new getClasses errored: %s", err)
	}

	results := make([]interface{}, len(classes), len(classes))
	for i, class := range classes {
		results[i] = j.doLocalGetClassResolveOneClass(params.Kind, className, class.uuid, params.Properties, class.properties)
	}

	return results, nil
}

func (j *Janusgraph) doLocalGetClassResolveOneClass(knd kind.Kind, className schema.ClassName,
	foundUUID strfmt.UUID, selectProperties []get.SelectProperty,
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
			// result[selectProperty.Name] = j.doLocalGetClassResolveRefClassProp(knd, className, selectProperty, propertiesMap)
			result[selectProperty.Name] = []interface{}{}
		}
	}

	return result
}

func (j *Janusgraph) doLocalGetClassResolveRefClassProp(knd kind.Kind, className schema.ClassName,

	selectProperty get.SelectProperty, propertiesMap map[string]interface{}) []interface{} {
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
		refString := rawRef["$cref"].(string)
		ref, err := crossref.Parse(refString)
		if err != nil {
			panic(fmt.Sprintf("janusgraph.LocalGetClass: could not parse crossref '%s': %s", refString, err))
		}

		if ref.Local {
			localRef := j.doLocalGetClassResolveLocalRef(selectProperty, ref.Kind, ref.TargetID)
			refResults = append(refResults, localRef...)
		} else {
			networkRef := extractNetworkRef(ref.Kind, ref.TargetID, ref.PeerName, selectProperty)
			refResults = append(refResults, networkRef...)
		}
	}

	return refResults
}

func (j *Janusgraph) doLocalGetClassResolveLocalRef(selectProperty get.SelectProperty,
	kind kind.Kind, refID strfmt.UUID) []interface{} {
	var refAtClass string
	var refPropertiesSchema models.Schema
	err := j.getClass(kind, refID, &refAtClass, nil, nil, nil, nil, &refPropertiesSchema)
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
		get.LocalRef{
			Fields:  localRef,
			AtClass: refAtClass,
		}}
}

func extractNetworkRef(k kind.Kind, refID strfmt.UUID, peerName string,
	selectProperty get.SelectProperty) []interface{} {

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
	if !selectProperty.HasPeer(peerName) {
		return []interface{}{}
	}

	return []interface{}{
		get.NetworkRef{
			NetworkKind: crossrefs.NetworkKind{
				PeerName: peerName,
				ID:       refID,
				Kind:     k,
			},
		},
	}
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
