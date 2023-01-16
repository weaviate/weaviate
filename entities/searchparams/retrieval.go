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

package searchparams

type NearVector struct {
	Vector       []float32 `json:"vector"`
	Certainty    float64   `json:"certainty"`
	Distance     float64   `json:"distance"`
	WithDistance bool      `json:"-"`
}

type KeywordRanking struct {
	Type       string   `json:"type"`
	Properties []string `json:"properties"`
	Query      string   `json:"query"`
}

type WeightedSearchResult struct {
	SearchParams interface{} `json:"searchParams"`
	Weight       float64     `json:"weight"`
	Type         string      `json:"type"`
}

type HybridSearch struct {
	SubSearches interface{} `json:"subSearches"`
	Type        string      `json:"type"`
	Limit       int         `json:"limit"`
	Alpha       float64     `json:"alpha"`
	Query       string      `json:"query"`
	Vector      []float32   `json:"vector"`
}

type NearObject struct {
	ID           string  `json:"id"`
	Beacon       string  `json:"beacon"`
	Certainty    float64 `json:"certainty"`
	Distance     float64 `json:"distance"`
	WithDistance bool    `json:"-"`
}

type ObjectMove struct {
	ID     string
	Beacon string
}

// ExploreMove moves an existing Search Vector closer (or further away from) a specific other search term
type ExploreMove struct {
	Values  []string
	Force   float32
	Objects []ObjectMove
}

type NearTextParams struct {
	Values       []string
	Limit        int
	MoveTo       ExploreMove
	MoveAwayFrom ExploreMove
	Certainty    float64
	Distance     float64
	WithDistance bool
	Network      bool
	Autocorrect  bool
}
