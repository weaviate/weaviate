package filterext

import (
	"encoding/json"
	"fmt"

	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
)

func parseValue(in *models.WhereFilter) (*filters.Value, error) {
	var value *filters.Value

	for _, extractor := range valueExtractors {
		foundValue, err := extractor(in)

		// Abort if we found a value, but it's for being passed a string to an int value.
		if err != nil {
			return nil, err
		}

		if foundValue != nil {
			if value != nil {
				return nil, fmt.Errorf("found more than one values the clause '%s'", jsonify(in))
			} else {
				value = foundValue
			}
		}
	}

	if value == nil {
		return nil, fmt.Errorf("no value set in filter '%s'", jsonify(in))
	}

	return value, nil
}

type valueExtractorFunc func(*models.WhereFilter) (*filters.Value, error)

var valueExtractors = []valueExtractorFunc{
	func(in *models.WhereFilter) (*filters.Value, error) {

		if in.ValueInt == nil {
			return nil, nil
		}

		return valueFilter(int(*in.ValueInt), schema.DataTypeInt), nil
	},
}

func valueFilter(value interface{}, dt schema.DataType) *filters.Value {
	return &filters.Value{
		Type:  dt,
		Value: value,
	}
}

// Small utility function used in printing error messages.
func jsonify(stuff interface{}) string {
	j, _ := json.Marshal(stuff)
	return string(j)
}
