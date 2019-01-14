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

	totalTrue, ok := boolPropsMap["true"]
	if !ok {
		return nil, fmt.Errorf(
			"boolProps did not contain any info about no of props which are 'true', got '%#v'", boolProps)
	}

	totalTrueInt, ok := totalTrue.(float64)
	if !ok {
		return nil, fmt.Errorf(
			"total true must be an float64, but we got '%t'", totalTrue)
	}

	totalFalse, ok := boolPropsMap["false"]
	if !ok {
		return nil, fmt.Errorf(
			"boolProps did not contain any info about no of props which are 'false', got '%#v'", boolProps)
	}

	totalFalseInt, ok := totalFalse.(float64)
	if !ok {
		return nil, fmt.Errorf(
			"total true must be an float64, but we got '%t'", totalFalse)
	}

	total := totalTrueInt + totalFalseInt

	return map[string]interface{}{
		"totalTrue":       totalTrue,
		"totalFalse":      totalFalse,
		"percentageTrue":  totalTrueInt / total,
		"percentageFalse": totalFalseInt / total,
	}, nil
}
