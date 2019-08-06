//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package meta

import (
	"fmt"
	"sort"
)

type stringOccurrence struct {
	value  string
	occurs float64
}

func (s stringOccurrence) Occurs() float64 {
	return s.occurs
}

// stringOccurrences implements sort.Interface
type stringOccurrences []stringOccurrence

func (c stringOccurrences) Len() int           { return len(c) }
func (c stringOccurrences) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }
func (c stringOccurrences) Less(i, j int) bool { return c[i].Occurs() < c[j].Occurs() }

// the contract with the graphQL api is untyped return values, i.e.
// map[string]interface{} or []interface{}. This method removes all higher
// types and returns those lower types
func (c stringOccurrences) stripTypes() []interface{} {
	result := make([]interface{}, len(c), len(c))
	for i, occurrence := range c {
		result[i] = map[string]interface{}{
			"value":  occurrence.value,
			"occurs": occurrence.occurs,
		}
	}

	return result
}

func (p *Processor) postProcessStringTopOccurrences(m map[string]interface{}) (map[string]interface{}, error) {
	// we know that this method is only ever called after asserting that the key
	// exists, so no need to check for it's existence again
	inner, _ := m[StringTopOccurrences]

	innerMap, ok := inner.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf(
			"expected topOccurrences to be a map, but was '%t'", inner)
	}

	occurrences, err := parseTopOccurrences(innerMap)
	if err != nil {
		return nil, fmt.Errorf(
			"could not parse top occurrences: %s", err)
	}

	sort.Sort(sort.Reverse(occurrences))

	topOccMap := map[string]interface{}{
		StringTopOccurrences: occurrences.stripTypes(),
	}

	delete(m, StringTopOccurrences)
	return mergeMaps(m, topOccMap), nil
}

func parseTopOccurrences(innerMap map[string]interface{}) (stringOccurrences, error) {
	occurrences := stringOccurrences{}
	for value, occurs := range innerMap {
		o, ok := occurs.(float64)
		if !ok {
			return nil, fmt.Errorf(
				"expected occurrence (value in map result from janus) to be an int, but was '%t'", occurs)
		}

		occurrences = append(occurrences, stringOccurrence{
			value:  value,
			occurs: o,
		})

	}
	return occurrences, nil
}
