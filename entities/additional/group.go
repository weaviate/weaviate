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

package additional

type Group struct {
	ID          int                      `json:"id"`
	GroupValue  string                   `json:"groupValue"`
	MinDistance float32                  `json:"minDistance"`
	MaxDistance float32                  `json:"maxDistance"`
	Count       int                      `json:"count"`
	Hits        []map[string]interface{} `json:"hits"`
}

type GroupHitAdditional struct {
	ID       string    `json:"id"`
	Vector   []float32 `json:"vector"`
	Distance float32   `json:"distance"`
}
