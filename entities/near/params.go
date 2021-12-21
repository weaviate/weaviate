//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package near

type NearVectorParams struct {
	Vector    []float32
	Certainty float64
}

type NearObjectParams struct {
	ID        string
	Beacon    string
	Certainty float64
}

// ExploreParams are the parameters used by the GraphQL `Explore { }` API
type ExploreParams struct {
	NearVector   *NearVectorParams
	NearObject   *NearObjectParams
	Offset       int
	Limit        int
	ModuleParams map[string]interface{}
}
