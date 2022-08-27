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

package neartext

import (
	"github.com/pkg/errors"
)

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

// ExploreMove moves an existing Search Vector closer (or further away from) a specific other search term
type ExploreMove struct {
	Values  []string
	Force   float32
	Objects []ObjectMove
}

type ObjectMove struct {
	ID     string
	Beacon string
}

func (g *GraphQLArgumentsProvider) validateNearTextFn(param interface{}) error {
	nearText, ok := param.(*NearTextParams)
	if !ok {
		return errors.New("'nearText' invalid parameter")
	}

	if nearText.MoveTo.Force > 0 &&
		nearText.MoveTo.Values == nil && nearText.MoveTo.Objects == nil {
		return errors.Errorf("'nearText.moveTo' parameter " +
			"needs to have defined either 'concepts' or 'objects' fields")
	}

	if nearText.MoveAwayFrom.Force > 0 &&
		nearText.MoveAwayFrom.Values == nil && nearText.MoveAwayFrom.Objects == nil {
		return errors.Errorf("'nearText.moveAwayFrom' parameter " +
			"needs to have defined either 'concepts' or 'objects' fields")
	}

	if nearText.Certainty != 0 && nearText.WithDistance {
		return errors.Errorf(
			"nearText cannot provide both distance and certainty")
	}

	return nil
}
