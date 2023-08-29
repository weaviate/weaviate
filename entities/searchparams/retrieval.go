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

package searchparams

import "github.com/pkg/errors"

type NearVector struct {
	Vector       []float32 `json:"vector"`
	Certainty    float64   `json:"certainty"`
	Distance     float64   `json:"distance"`
	WithDistance bool      `json:"-"`
}

type KeywordRanking struct {
	Type                   string   `json:"type"`
	Properties             []string `json:"properties"`
	Query                  string   `json:"query"`
	AdditionalExplanations bool     `json:"additionalExplanations"`
}

type WeightedSearchResult struct {
	SearchParams interface{} `json:"searchParams"`
	Weight       float64     `json:"weight"`
	Type         string      `json:"type"`
}

type HybridSearch struct {
	SubSearches     interface{} `json:"subSearches"`
	Type            string      `json:"type"`
	Alpha           float64     `json:"alpha"`
	Query           string      `json:"query"`
	Vector          []float32   `json:"vector"`
	Properties      []string    `json:"properties"`
	FusionAlgorithm int         `json:"fusionalgorithm"`
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

func (n NearTextParams) GetCertainty() float64 {
	return n.Certainty
}

func (n NearTextParams) GetDistance() float64 {
	return n.Distance
}

func (n NearTextParams) SimilarityMetricProvided() bool {
	return n.Certainty != 0 || n.WithDistance
}

func (n NearTextParams) Validate() error {
	if n.MoveTo.Force > 0 &&
		n.MoveTo.Values == nil && n.MoveTo.Objects == nil {
		return errors.Errorf("'nearText.moveTo' parameter " +
			"needs to have defined either 'concepts' or 'objects' fields")
	}

	if n.MoveAwayFrom.Force > 0 &&
		n.MoveAwayFrom.Values == nil && n.MoveAwayFrom.Objects == nil {
		return errors.Errorf("'nearText.moveAwayFrom' parameter " +
			"needs to have defined either 'concepts' or 'objects' fields")
	}

	if n.Certainty != 0 && n.WithDistance {
		return errors.Errorf(
			"nearText cannot provide both distance and certainty")
	}

	return nil
}

type GroupBy struct {
	Property        string
	Groups          int
	ObjectsPerGroup int
}
