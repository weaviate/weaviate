//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
//  DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package meta

import "fmt"

func (p *Processor) postProcessBoolGroupCount(m map[string]interface{}) (map[string]interface{}, error) {
	totalTrue, err := getTotal(m, "true")
	if err != nil {
		return nil, err
	}

	totalFalse, err := getTotal(m, "false")
	if err != nil {
		return nil, err
	}

	total := totalTrue + totalFalse

	boolAnalysis := map[string]interface{}{
		"totalTrue":       totalTrue,
		"totalFalse":      totalFalse,
		"percentageTrue":  totalTrue / total,
		"percentageFalse": totalFalse / total,
	}

	delete(m, "true")
	delete(m, "false")
	return mergeMaps(m, boolAnalysis), nil
}

func getTotal(m map[string]interface{}, key string) (float64, error) {
	total, ok := m[key]
	if !ok {
		// this would happen if every single prop is "false"
		total = float64(0)
	}

	totalFloat, ok := total.(float64)
	if !ok {
		return 0.0, fmt.Errorf(
			"total %s must be an float64, but we got '%t'", key, total)
	}

	return totalFloat, nil
}
