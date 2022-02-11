//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package traverser

import (
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/search"
)

type GetParams struct {
	Filters              *filters.LocalFilter
	ClassName            string
	Pagination           *filters.Pagination
	Properties           search.SelectProperties
	NearVector           *NearVectorParams
	NearObject           *NearObjectParams
	KeywordRanking       *KeywordRankingParams
	SearchVector         []float32
	Group                *GroupParams
	ModuleParams         map[string]interface{}
	AdditionalProperties additional.Properties
}

type GroupParams struct {
	Strategy string
	Force    float32
}
