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
package meta

import "fmt"

func (p *Processor) postProcessBoolGroupCount(m map[string]interface{}) (map[string]interface{}, error) {
	// we can safely ignore the check, as this method is only ever called if we
	// can be sure that we have this prop
	boolProps, _ := m[BoolGroupCount]

	boolPropsMap, ok := boolProps.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf(
			"could not post-process boolean totals, expected result to be a map, but was '%#v'", boolProps)
	}

	totalTrue, err := getTotal(boolPropsMap, "true")
	if err != nil {
		return nil, err
	}

	totalFalse, err := getTotal(boolPropsMap, "false")
	if err != nil {
		return nil, err
	}

	total := totalTrue + totalFalse

	return map[string]interface{}{
		"totalTrue":       totalTrue,
		"totalFalse":      totalFalse,
		"percentageTrue":  totalTrue / total,
		"percentageFalse": totalFalse / total,
	}, nil
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
