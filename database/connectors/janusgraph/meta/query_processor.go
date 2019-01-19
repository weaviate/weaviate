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

func (p *Processor) Process(query *gremlin.Query, typeInfo map[string]interface{}) (interface{}, error) {
	result, err := p.executor.Execute(query)
	if err != nil {
		return nil, fmt.Errorf("could not process meta query: executing the query failed: %s", err)
	}

	merged, err := p.mergeResults(result, typeInfo)
	if err != nil {
		return nil, fmt.Errorf(
			"got a successful response from janus, but could not merge the results: %s", err)
	}

	return merged, nil
}

func (p *Processor) mergeResults(input *gremlin.Response,
	typeInfo map[string]interface{}) (interface{}, error) {
	result := map[string]interface{}{}

	for _, datum := range input.Data {
		datumAsMap, ok := datum.Datum.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("expected datum to be map, but was %#v", datum.Datum)
		}

		// merge datums from janus
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

	// merge in type info
	for k, v := range typeInfo {
		if _, ok := result[k]; !ok {
			// first time we see this prop
			result[k] = v
		} else {
			typeMap, ok := v.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("type property was not a map, but %t", v)

			}
			// we had this prop before let's merge them
			result[k] = mergeMaps(result[k].(map[string]interface{}), typeMap)
		}
	}

	return map[string]interface{}(result), nil
}

func (p *Processor) postProcess(m map[string]interface{}) (map[string]interface{}, error) {
	for k := range m {
		switch k {
		case BoolGroupCount:
			return p.postProcessBoolGroupCount(m)
		case StringTopOccurrences:
			return p.postProcessStringTopOccurrences(m)
		}
	}

	return m, nil
}

// m2 takes precedence in case of a conflict
func mergeMaps(m1, m2 map[string]interface{}) map[string]interface{} {
	for k, v := range m2 {
		m1[k] = v
	}

	return m1
}
