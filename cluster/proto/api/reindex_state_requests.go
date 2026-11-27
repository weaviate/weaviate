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

package api

const (
	ReindexFilterable = "filterable"
	ReindexSearchable = "searchable"
	ReindexRangeable  = "rangeable"
)

// ReindexFlipRequest makes generation Gen of Prop's IndexTypes the class's own,
// with the run's target settings, once every tenant serves it.
type ReindexFlipRequest struct {
	Prop         string
	Gen          uint64
	Tokenization string // empty keeps the property's
	IndexTypes   []string
	Enable       bool
	Blockmax     bool // stamps the searchable index
}
