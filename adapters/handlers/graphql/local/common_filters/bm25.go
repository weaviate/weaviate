package common_filters

import "github.com/semi-technologies/weaviate/usecases/traverser"

// ExtractBM25
func ExtractBM25(source map[string]interface{}) traverser.KeywordRankingParams {
	var args traverser.KeywordRankingParams

	p, ok := source["properties"]
	if ok {
		rawSlice := p.([]interface{})
		args.Properties = make([]string, len(rawSlice))
		for i, raw := range rawSlice {
			args.Properties[i] = raw.(string)
		}
	}

	query, ok := source["query"]
	if ok {
		args.Query = query.(string)
	}

	return args
}
