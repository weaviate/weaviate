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

package additional

import "github.com/go-openapi/strfmt"

type Group struct {
	ID          int                      `json:"id"`
	GroupedBy   *GroupedBy               `json:"groupedBy"`
	MinDistance float32                  `json:"minDistance"`
	MaxDistance float32                  `json:"maxDistance"`
	Count       int                      `json:"count"`
	Hits        []map[string]interface{} `json:"hits"`
}

type GroupedBy struct {
	Value string   `json:"value"`
	Path  []string `json:"path"`
}

type GroupHitAdditional struct {
	ID       strfmt.UUID `json:"id"`
	Vector   []float32   `json:"vector"`
	Distance float32     `json:"distance"`
}
