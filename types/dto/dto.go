package dto

import (
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
)

type GroupParams struct {
	Strategy string
	Force    float32
}

type GetParams struct {
	Filters              *filters.LocalFilter
	ClassName            string
	Pagination           *filters.Pagination
	Sort                 []filters.Sort
	Properties           search.SelectProperties
	NearVector           *searchparams.NearVector
	NearObject           *searchparams.NearObject
	KeywordRanking       *searchparams.KeywordRanking
	HybridSearch         *searchparams.HybridSearch
	SearchVector         []float32
	Group                *GroupParams
	ModuleParams         map[string]interface{}
	AdditionalProperties additional.Properties
}