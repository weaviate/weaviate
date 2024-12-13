//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
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

type TargetCombinationType int

const (
	Sum TargetCombinationType = iota
	Average
	Minimum
	ManualWeights
	RelativeScore
)

// no weights are set for default, needs to be added if this is changed to something else
const DefaultTargetCombinationType = Minimum

type TargetCombination struct {
	// just one of these can be set, precedence in order
	Type    TargetCombinationType
	Weights []float32
}

type GetParams struct {
	Filters                 *filters.LocalFilter
	ClassName               string
	Pagination              *filters.Pagination
	Cursor                  *filters.Cursor
	Sort                    []filters.Sort
	Properties              search.SelectProperties
	NearVector              *searchparams.NearVector
	NearObject              *searchparams.NearObject
	KeywordRanking          *searchparams.KeywordRanking
	HybridSearch            *searchparams.HybridSearch
	GroupBy                 *searchparams.GroupBy
	TargetVector            string
	TargetVectorCombination *TargetCombination
	Group                   *GroupParams
	ModuleParams            map[string]interface{}
	AdditionalProperties    additional.Properties
	ReplicationProperties   *additional.ReplicationProperties
	Tenant                  string
	IsRefOrigin             bool // is created by ref filter
}
