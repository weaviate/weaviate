//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

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
	Filters               *filters.LocalFilter
	ClassName             string
	Pagination            *filters.Pagination
	Cursor                *filters.Cursor
	Sort                  []filters.Sort
	Properties            search.SelectProperties
	NearVector            *searchparams.NearVector
	NearObject            *searchparams.NearObject
	KeywordRanking        *searchparams.KeywordRanking
	HybridSearch          *searchparams.HybridSearch
	SearchVector          []float32
	Group                 *GroupParams
	GroupBy               *GroupByParams
	ModuleParams          map[string]interface{}
	AdditionalProperties  additional.Properties
	ReplicationProperties *additional.ReplicationProperties
}

type GroupByParams struct {
	Prop            string
	ObjectsPerGroup int
	GroupsLimit     int
}
