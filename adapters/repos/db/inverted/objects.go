package inverted

import (
	"fmt"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
)

func (a *Analyzer) Object(input map[string]interface{}, props []*models.Property) ([]Property, error) {
	propsMap := map[string]*models.Property{}
	for _, prop := range props {
		propsMap[prop.Name] = prop
	}

	out := make([]Property, len(input))
	i := 0
	for key, value := range input {
		prop, ok := propsMap[key]
		if !ok {
			// not a registered property, skip
			continue
		}

		if len(prop.DataType) != 1 {
			// must be a ref prop or something else is wrong, skip
			continue
		}

		var items []Countable
		switch schema.DataType(prop.DataType[0]) {
		case schema.DataTypeText:
			asString, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("expected property %s to be of type string, but got %T", key, value)
			}
			items = a.Text(asString)
		case schema.DataTypeString:
			asString, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("expected property %s to be of type string, but got %T", key, value)
			}
			items = a.String(asString)
		default:
			// ignore unsupported prop type
			continue
		}

		out[i] = Property{
			Name:  key,
			Items: items,
		}
		i++
	}

	return out[:i], nil
}
