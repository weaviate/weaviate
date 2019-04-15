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

package get

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/creativesoftwarefdn/weaviate/database/connectors/janusgraph/state"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/crossref"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/local/get"
	"github.com/creativesoftwarefdn/weaviate/gremlin"
	"github.com/creativesoftwarefdn/weaviate/usecases/network/crossrefs"
	"github.com/go-openapi/strfmt"
)

// Processor is a simple Gremlin-Query Executor that is specific to Fetch. It
// transforms the return value into a usable beacon structure and calculates
// the final certainty.
type Processor struct {
	executor   executor
	nameSource nameSource
	className  schema.ClassName
}

type executor interface {
	Execute(query gremlin.Gremlin) (*gremlin.Response, error)
}

var isAProp *regexp.Regexp

func init() {
	isAProp = regexp.MustCompile("^prop_")
}

//NewProcessor from a gremlin executer. See Processor for details.
func NewProcessor(executor executor, nameSource nameSource, className schema.ClassName) *Processor {
	return &Processor{executor: executor, nameSource: nameSource, className: className}
}

// Process the query by executing it and then transforming the results to
// include the beacon structure
func (p *Processor) Process(query *gremlin.Query) ([]interface{}, error) {
	result, err := p.executor.Execute(query)
	if err != nil {
		return nil, fmt.Errorf("could not process fetch query: executing the query failed: %s", err)
	}

	results := []interface{}{}
	for i, datum := range result.Data {
		processed, err := p.processDatum(datum)
		if err != nil {
			return nil, fmt.Errorf("could not process datum at position %d: %s", i, err)
		}

		results = append(results, processed)
	}

	return nestedDedup(results), nil
}

func (p *Processor) processDatum(d gremlin.Datum) (interface{}, error) {
	datumMap, ok := d.Datum.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("expected datum to be a map, but was %T", d)
	}

	objects, ok := datumMap["objects"]
	if !ok {
		return nil, fmt.Errorf("expected datum map to have key objects, but was %#v", datumMap)
	}

	objectsSlice, ok := objects.([]interface{})
	if !ok {
		return nil, fmt.Errorf("expected objects to be a slice, but was %T", objects)
	}

	if len(objectsSlice)%2 != 1 {
		return nil, fmt.Errorf("length of objects has to be an odd number to match pattern v or v-e-v, "+
			"or v-e-v-e-v, etc., but got len %d", len(objectsSlice))
	}

	var refProps map[string]interface{}
	var err error
	if len(objectsSlice) > 1 {
		// we seem to have a cross-ref
		refProps, err = p.processEdgeAndVertexObjects(objectsSlice[1:], p.className)
		if err != nil {
			return nil, fmt.Errorf("could not process ref props: %s", err)
		}
	}

	vertex, err := p.processVertexObject(objectsSlice[0])
	if err != nil {
		return nil, fmt.Errorf("could not process vertex (without cross-refs): %s", err)
	}

	return mergeMaps(vertex, refProps), nil

}

func (p *Processor) processVertexObject(o interface{}) (map[string]interface{}, error) {
	objMap, ok := o.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("expected object to be a map, but was %T", o)
	}

	className, err := p.classNameFromVertex(objMap)
	if err != nil {
		return nil, fmt.Errorf("could not identify what class the vertex is: %s", err)
	}

	results := map[string]interface{}{}
	for key, value := range objMap {
		if key == "classId" {
			continue
		}

		prop, err := assumeSliceAndExtractFirst(value)
		if err != nil {
			return nil, fmt.Errorf("for key '%s': %s", key, err)
		}

		if !isAProp.MatchString(key) {
			results[key] = prop
			continue
		}

		prop, err = p.parseProp(prop)
		if err != nil {
			return nil, fmt.Errorf("could not parse prop '%s': %s", key, err)
		}

		readablePropName, err := p.nameSource.GetPropertyNameFromMapped(className, state.MappedPropertyName(key))
		if err != nil {
			// this property might have been deleted in the meantime, let's skip it
			continue
		}

		results[string(readablePropName)] = prop
	}

	return results, nil
}

func (p *Processor) classNameFromVertex(v map[string]interface{}) (schema.ClassName, error) {
	classID, ok := v["classId"]
	if !ok {
		return "", fmt.Errorf("vertex doesn't have prop 'classId'")
	}

	class, err := assumeSliceAndExtractFirst(classID)
	if err != nil {
		return "", fmt.Errorf("prop 'classId': %s", err)
	}

	return p.nameSource.GetClassNameFromMapped(state.MappedClassName(class.(string))), nil
}

func (p *Processor) parseProp(prop interface{}) (interface{}, error) {
	switch v := prop.(type) {
	case map[string]interface{}:
		return p.parseMapProp(v)
	default:
		// most props don't need special parsing
		return prop, nil
	}
}

func (p *Processor) parseMapProp(prop map[string]interface{}) (interface{}, error) {
	// as of now the only supported map prop would be a geo shape point:
	return (&gremlin.PropertyValue{Value: prop}).GeoCoordinates()
}

func (p *Processor) processEdgeAndVertexObjects(o []interface{}, className schema.ClassName) (map[string]interface{}, error) {
	edge := o[0]

	edgeMap, ok := edge.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("expected edge object to be a map, but was %T", edge)
	}

	refProp, ok := edgeMap["refId"]
	if !ok {
		return nil, fmt.Errorf("expected edge object to be have key 'refId', but got %#v", edgeMap)
	}

	ref, err := p.refTypeFromEdge(edgeMap)
	if err != nil {
		return nil, fmt.Errorf("could not identiy ref type for class '%s': %s", className, err)
	}

	crossRefProp := string(p.nameSource.MustGetPropertyNameFromMapped(className, state.MappedPropertyName(refProp.(string))))

	if ref.Local {
		return p.processLocalRefEdge(o, crossRefProp, className)
	}

	return p.processNetworkRefEdge(ref, crossRefProp)
}

func (p *Processor) refTypeFromEdge(edgeMap map[string]interface{}) (*crossref.Ref, error) {
	uuid, ok := edgeMap["$cref"]
	if !ok {
		return nil, fmt.Errorf("expected edge object to be have key '$cref', but got %#v", edgeMap)
	}

	location, ok := edgeMap["locationUrl"]
	if !ok {
		return nil, fmt.Errorf("expected edge object to be have key 'locationUrl', but got %#v", edgeMap)
	}

	refType, ok := edgeMap["refType"]
	if !ok {
		return nil, fmt.Errorf("expected edge object to be have key 'refType', but got %#v", edgeMap)
	}

	ref := crossref.New(location.(string), strfmt.UUID(uuid.(string)), kind.KindByName(refType.(string)))
	return ref, nil
}

func (p *Processor) processLocalRefEdge(o []interface{}, crossRefProp string,
	className schema.ClassName) (map[string]interface{}, error) {
	processedVertex, err := p.processVertexObject(o[1])
	if err != nil {
		return nil, fmt.Errorf("could not process vertex (of cross-ref): %s", err)
	}

	linkedClassName, err := p.classNameFromVertex(o[1].(map[string]interface{}))
	if err != nil {
		return nil, fmt.Errorf("could not extract class name from linked vertex: %s", err)
	}

	if len(o) > 2 {
		// this means the nesting continues
		nestedRefMap, err := p.processEdgeAndVertexObjects(o[2:], linkedClassName)
		if err != nil {
			return nil, fmt.Errorf("could not process cross-ref from %s to %s: %s", className, linkedClassName, err)
		}
		processedVertex = mergeMaps(processedVertex, nestedRefMap)
	}

	return map[string]interface{}{
		strings.Title(crossRefProp): []interface{}{get.LocalRef{Fields: processedVertex, Class: string(linkedClassName)}},
	}, nil
}

func (p *Processor) processNetworkRefEdge(ref *crossref.Ref, propName string) (map[string]interface{}, error) {
	return map[string]interface{}{
		strings.Title(propName): []interface{}{get.NetworkRef{
			NetworkKind: crossrefs.NetworkKind{
				PeerName: ref.PeerName,
				ID:       ref.TargetID,
				Kind:     ref.Kind,
			},
		},
		},
	}, nil
}

func mergeMaps(m1, m2 map[string]interface{}) map[string]interface{} {
	for k, v := range m2 {
		m1[k] = v
	}

	return m1
}

func assumeSliceAndExtractFirst(v interface{}) (interface{}, error) {
	valueSlice, ok := v.([]interface{})
	if !ok {
		return nil, fmt.Errorf("expected value  to be a slice, but got %T", v)
	}

	if len(valueSlice) != 1 {
		return nil, fmt.Errorf("expected value to be of length 1 for , but got %d", len(valueSlice))
	}

	return valueSlice[0], nil
}
