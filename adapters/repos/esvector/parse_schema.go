package esvector

import (
	"fmt"

	"github.com/semi-technologies/weaviate/entities/models"
)

// parseSchema lightly parses the schema, while most fields stay untyped, those
// with special meaning, such as GeoCoordinates are marshalled into their
// required types
func parseSchema(input map[string]interface{}) (map[string]interface{}, error) {
	output := map[string]interface{}{}

	for key, value := range input {
		switch typed := value.(type) {
		case map[string]interface{}:
			parsed, err := parseMapProp(typed)
			if err != nil {
				return output, fmt.Errorf("prop '%s': %v", key, err)
			}

			output[key] = parsed

		default:
			// anything else remains unchanged
			output[key] = value
		}
	}

	return output, nil
}

func parseMapProp(input map[string]interface{}) (interface{}, error) {
	lat, latOK := input["lat"]
	lon, lonOK := input["lon"]

	if latOK && lonOK {
		// this is a geoCoordinates prop
		return parseGeoProp(lat, lon)
	}

	return nil, fmt.Errorf("unknown map prop which is not a geo prop: %v", input)
}

func parseGeoProp(lat interface{}, lon interface{}) (*models.GeoCoordinates, error) {
	latFloat, ok := lat.(float64)
	if !ok {
		return nil, fmt.Errorf("explected lat to be float64, but is %T", lat)
	}

	lonFloat, ok := lon.(float64)
	if !ok {
		return nil, fmt.Errorf("explected lon to be float64, but is %T", lon)
	}

	return &models.GeoCoordinates{Latitude: float32(latFloat), Longitude: float32(lonFloat)}, nil
}
