package common_filters

import "github.com/weaviate/weaviate/entities/searchparams"

// ExtractEF arguments, such as "DynamicMin" and "DynamicMax"
func ExtractEF(source map[string]interface{}) (searchparams.EF, error) {
	var args searchparams.EF

	DynamicMin, minOk := source["dynamicMin"]
	if minOk {
		args.DynamicMin = DynamicMin.(int)
	}

	dynamicMin, maxOk := source["dynamicMax"]
	if maxOk {
		args.DynamicMax = dynamicMin.(int)
	}

	factor, factorOk := source["dynamicFactor"]
	if factorOk {
		args.DynamicFactor = factor.(int)
	}

	if !minOk && !maxOk && !factorOk {
		return args, nil
	}

	args.Enable = true

	return args, nil
}
