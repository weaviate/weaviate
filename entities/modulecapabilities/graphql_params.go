//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package modulecapabilities

type NearTextParams struct {
	Values       []string
	Limit        int
	MoveTo       ExploreMove
	MoveAwayFrom ExploreMove
	Certainty    float64
	Network      bool
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
