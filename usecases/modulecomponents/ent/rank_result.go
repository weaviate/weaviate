//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package ent

import "github.com/go-openapi/strfmt"

// Document is one search result handed to a rerank client: the object's ID
// and the value of the rerank property, "" when the object has none.
type Document struct {
	ID    strfmt.UUID
	Value string
}

type RankResult struct {
	Query          string
	DocumentScores []DocumentScore
}

type DocumentScore struct {
	Document string
	Score    float64
}
