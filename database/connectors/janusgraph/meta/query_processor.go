package meta

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/gremlin"
)

// Processor is a simple Gremlin-Query Executor that is specific to GetMeta in
// that it merges the results into the expected format and applies
// post-processing where necessary
type Processor struct {
	executor executor
}

type executor interface {
	Execute(query gremlin.Gremlin) (*gremlin.Response, error)
}

func NewProcessor(executor executor) *Processor {
	return &Processor{executor: executor}
}

func (p *Processor) Process(query *gremlin.Query) (interface{}, error) {

	result, err := p.executor.Execute(query)
	if err != nil {
		return nil, fmt.Errorf("could not process meta query: executing the query failed: %s", err)
	}

	merged, err := p.mergeResults(result)
	if err != nil {
		return nil, fmt.Errorf(
			"got a successful response from janus, but could not merge the results: %s", err)
	}

	return merged, nil
}

func (p *Processor) mergeResults(input *gremlin.Response) (interface{}, error) {
	result := map[string]interface{}{}

	for _, datum := range input.Data {
		datumAsMap, ok := datum.Datum.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("expected datum to be map, but was %#v", datum.Datum)
		}

		for k, v := range datumAsMap {
			vAsMap, ok := v.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("expected property to be map, but was %#v", v)
			}

			processed, err := p.postProcess(vAsMap)
			if err != nil {
				return nil, fmt.Errorf("could not postprocess '%#v': %s", v, err)
			}

			if _, ok := result[k]; !ok {
				// first time we see this prop
				result[k] = processed
			} else {
				// we had this prop before let's merge them

				result[k] = mergeMaps(result[k].(map[string]interface{}), processed)
			}
		}
	}

	return map[string]interface{}(result), nil
}

func (p *Processor) postProcess(m map[string]interface{}) (map[string]interface{}, error) {
	for k := range m {
		switch k {
		case BoolGroupCount:
			return p.postProcessBoolGroupCount(m)
		}
	}

	return m, nil
}

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

// m2 takes precedence in case of a conflict
func mergeMaps(m1, m2 map[string]interface{}) map[string]interface{} {
	for k, v := range m2 {
		m1[k] = v
	}

	return m1
}
