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

	"github.com/creativesoftwarefdn/weaviate/database/connectors/janusgraph/state"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/gremlin"
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

	return results, nil
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

	if len(objectsSlice) != 1 {
		return nil, fmt.Errorf("only non-cross refs supported for now, but got length %d", len(objectsSlice))
	}

	return p.processVertexObject(objectsSlice[0])
}

func (p *Processor) processVertexObject(o interface{}) (interface{}, error) {
	objMap, ok := o.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("expected object to be a map, but was %T", o)
	}

	results := map[string]interface{}{}
	for key, value := range objMap {
		valueSlice, ok := value.([]interface{})
		if !ok {
			return nil, fmt.Errorf("expected value for key '%s' to be a slice, but got %T", key, value)
		}

		if len(valueSlice) != 1 {
			return nil, fmt.Errorf("expected value to be of length 1 for key '%s', but got %d", key, len(valueSlice))
		}

		if !isAProp.MatchString(key) {
			results[key] = valueSlice[0]
			continue
		}

		readablePropName := string(p.nameSource.GetPropertyNameFromMapped(p.className, state.MappedPropertyName(key)))
		results[readablePropName] = valueSlice[0]
	}

	return results, nil
}
