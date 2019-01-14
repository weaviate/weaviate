package meta

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/getmeta"
)

// TypeInspector can process the types of each specified props
type TypeInspector struct {
	typeSource typeSource
}

// NewTypeInspector to process the types of MetaPoperties
func NewTypeInspector(typeSource typeSource) *TypeInspector {
	return &TypeInspector{typeSource: typeSource}
}

// Process returns a simple map where each property is the key, the value
// contains the analysis prop that the user asked for through the graphQL API
func (t *TypeInspector) Process(params *getmeta.Params) (map[string]interface{}, error) {
	result := map[string]interface{}{}
	for _, prop := range params.Properties {

		propResult, err := t.analyzeAll(params, prop)
		if err != nil {
			return nil, err
		}

		if propResult != nil {
			result[string(prop.Name)] = propResult
		}
	}

	return result, nil
}

func (t *TypeInspector) analyzeAll(params *getmeta.Params,
	prop getmeta.MetaProperty) (map[string]interface{}, error) {
	for _, analysis := range prop.StatisticalAnalyses {
		result, err := t.analyze(params, prop, analysis)
		if err != nil {
			return nil, err
		}

		if result != nil {
			// as soon as we have one non-nil result we can exit early
			return result, nil
		}
	}

	return nil, nil
}

func (t *TypeInspector) analyze(params *getmeta.Params, prop getmeta.MetaProperty,
	analysis getmeta.StatisticalAnalysis) (map[string]interface{}, error) {
	switch analysis {
	case getmeta.PointingTo:
		return t.analyzeRefProp(params, prop, analysis)
	default:
		return nil, nil
	}
}

func (t *TypeInspector) analyzeRefProp(params *getmeta.Params, prop getmeta.MetaProperty,
	analysis getmeta.StatisticalAnalysis) (map[string]interface{}, error) {
	err, schemaProp := t.typeSource.GetProperty(params.Kind, params.ClassName, untitle(prop.Name))
	if err != nil {
		return nil, fmt.Errorf(
			"type inspector cannot get property %s.%s: %s", params.ClassName, prop.Name, err)
	}

	propType, err := t.typeSource.FindPropertyDataType(schemaProp.AtDataType)
	if err != nil {
		return nil, fmt.Errorf(
			"type inspector cannot get data type of property %s.%s: %s", params.ClassName, prop.Name, err)
	}

	if !propType.IsReference() {
		return nil, fmt.Errorf("asked for pointingTo, but don't have a ref prop: %#v", propType)
	}

	return map[string]interface{}{
		string(getmeta.PointingTo): classSliceToInterfaceSlice(propType.Classes()),
	}, nil
}

func classSliceToInterfaceSlice(classes []schema.ClassName) []interface{} {
	result := make([]interface{}, len(classes), len(classes))
	for i, className := range classes {
		result[i] = string(className)
	}

	return result
}
