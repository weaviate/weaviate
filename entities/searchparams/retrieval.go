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
	Vector    []float32 `json:"vector"`
	Certainty float64   `json:"certainty"`
	Distance  float64   `json:"distance"`
}

type KeywordRanking struct {
	Type       string   `json:"type"`
	Properties []string `json:"properties"`
	Query      string   `json:"query"`
}

type NearObject struct {
	ID        string  `json:"id"`
	Beacon    string  `json:"beacon"`
	Certainty float64 `json:"certainty"`
	Distance  float64 `json:"distance"`
}
