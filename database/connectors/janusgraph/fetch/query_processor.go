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

package fetch

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/database/connectors/janusgraph/state"
	"github.com/creativesoftwarefdn/weaviate/entities/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/gremlin"
)

// Processor is a simple Gremlin-Query Executor that is specific to Fetch. It
// transforms the return value into a usable beacon structure and calculates
// the final certainty.
type Processor struct {
	executor   executor
	kind       kind.Kind
	nameSource nameSource
	peerName   string
}

type executor interface {
	Execute(query gremlin.Gremlin) (*gremlin.Response, error)
}

//NewProcessor from a gremlin executer. See Processor for details.
func NewProcessor(executor executor, k kind.Kind, peer string, ns nameSource) *Processor {
	return &Processor{executor: executor, kind: k, peerName: peer, nameSource: ns}
}

// Process the query by executing it and then transforming the results to
// include the beacon structure
func (p *Processor) Process(query *gremlin.Query) (interface{}, error) {
	result, err := p.executor.Execute(query)
	if err != nil {
		return nil, fmt.Errorf("could not process fetch query: executing the query failed: %s", err)
	}

	results := make([]interface{}, len(result.Data), len(result.Data))
	for i, datum := range result.Data {
		beacon, err := p.extractBeacon(datum.Datum)
		if err != nil {
			return nil, fmt.Errorf("could not extract beacon: %s", err)
		}

		className, err := p.extractClassName(datum.Datum)
		if err != nil {
			return nil, fmt.Errorf("could not extract beacon: %s", err)
		}

		results[i] = map[string]interface{}{
			"beacon":    beacon,
			"className": className,
		}
	}

	return results, nil
}

func (p *Processor) extractBeacon(data interface{}) (string, error) {
	dataMap, ok := data.(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("expected datum to be a map, but was %T", data)
	}

	uuidString, err := p.getProperty(dataMap, "uuid")
	if err != nil {
		return "", err
	}

	return p.beaconFromUUID(uuidString)
}

func (p *Processor) extractClassName(data interface{}) (string, error) {
	dataMap, ok := data.(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("expected datum to be a map, but was %T", data)
	}

	id, err := p.getProperty(dataMap, "classId")
	if err != nil {
		return "", err
	}

	return p.classNameFromID(id)
}

func (p *Processor) getProperty(dataMap map[string]interface{}, propName string) (string, error) {
	prop, ok := dataMap[propName]
	if !ok {
		return "", fmt.Errorf("expected datum map to have a prop '%s', but got '%#v'",
			propName, dataMap)
	}

	propSlice, ok := prop.([]interface{})
	if !ok {
		return "", fmt.Errorf("expected prop 'uuid' to be a slice, but got '%#v'",
			prop)
	}

	if len(propSlice) != 1 {
		return "", fmt.Errorf("expected prop 'prop' have len of 1, but got '%#v'",
			propSlice)
	}

	propString, ok := propSlice[0].(string)
	if !ok {
		return "", fmt.Errorf("expected prop[0] to be a string, but got '%#v'",
			propSlice[0])
	}

	return propString, nil
}

func (p *Processor) beaconFromUUID(uuid string) (string, error) {
	return fmt.Sprintf("weaviate://%s/%ss/%s",
		p.peerName, p.kind.Name(), uuid), nil
}

func (p *Processor) classNameFromID(id string) (string, error) {
	return string(p.nameSource.GetClassNameFromMapped(state.MappedClassName(id))), nil
}
