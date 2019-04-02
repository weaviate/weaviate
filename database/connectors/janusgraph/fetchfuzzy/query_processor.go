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

package fetchfuzzy

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/gremlin"
)

// Processor is a simple Gremlin-Query Executor that is specific to Fetch. It
// transforms the return value into a usable beacon structure and calculates
// the final certainty.
type Processor struct {
	executor executor
	kind     kind.Kind
	peerName string
}

type executor interface {
	Execute(query gremlin.Gremlin) (*gremlin.Response, error)
}

//NewProcessor from a gremlin executer. See Processor for details.
func NewProcessor(executor executor, k kind.Kind, peer string) *Processor {
	return &Processor{executor: executor, kind: k, peerName: peer}
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

		results[i] = map[string]interface{}{
			"beacon": beacon,
		}
	}

	return results, nil
}

func (p *Processor) extractBeacon(data interface{}) (string, error) {
	dataMap, ok := data.(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("expected datum to be a map, but was %T", data)
	}

	uuid, ok := dataMap["uuid"]
	if !ok {
		return "", fmt.Errorf("expected datum map to have a prop 'uuid', but got '%#v'",
			dataMap)
	}

	uuidSlice, ok := uuid.([]interface{})
	if !ok {
		return "", fmt.Errorf("expected prop 'uuid' to be a slice, but got '%#v'",
			uuid)
	}

	if len(uuidSlice) != 1 {
		return "", fmt.Errorf("expected prop 'uuid' have len of 1, but got '%#v'",
			uuidSlice)
	}

	uuidString, ok := uuidSlice[0].(string)
	if !ok {
		return "", fmt.Errorf("expected uuid[0] to be a string, but got '%#v'",
			uuidSlice[0])
	}

	return p.beaconFromUUID(uuidString)
}

func (p *Processor) beaconFromUUID(uuid string) (string, error) {
	return fmt.Sprintf("weaviate://%s/%ss/%s",
		p.peerName, p.kind.Name(), uuid), nil
}
