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

package objects

type FeatureProjection struct {
	Vector []float32 `json:"vector"`
}

type NearestNeighbors struct {
	Neighbors []*NearestNeighbor `json:"neighbors"`
}

type NearestNeighbor struct {
	Concept  string    `json:"concept,omitempty"`
	Distance float32   `json:"distance,omitempty"`
	Vector   []float32 `json:"vector"`
}
