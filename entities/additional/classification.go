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

package additional

import (
	"maps"
	"slices"

	"github.com/go-openapi/strfmt"
)

type Classification struct {
	BasedOn          []string        `json:"basedOn"`
	ClassifiedFields []string        `json:"classifiedFields"`
	Completed        strfmt.DateTime `json:"completed,omitempty"`
	ID               strfmt.UUID     `json:"id,omitempty"`
	Scope            []string        `json:"scope"`
}

type Properties struct {
	Classification bool     `json:"classification"`
	RefMeta        bool     `json:"refMeta"`
	Vector         bool     `json:"vector"`
	Vectors        []string `json:"vectors"`
	// IncludeAllTargetVectors indicates that ALL target vectors should be included,
	// regardless of the Vectors field. This is used internally by MarshalBinary
	// to include all vectors for persistence. When false (default), the Vectors
	// field determines which specific vectors to include (empty means none).
	IncludeAllTargetVectors bool                   `json:"-"`
	Certainty               bool                   `json:"certainty"`
	ID                      bool                   `json:"id"`
	CreationTimeUnix        bool                   `json:"creationTimeUnix"`
	LastUpdateTimeUnix      bool                   `json:"lastUpdateTimeUnix"`
	ModuleParams            map[string]interface{} `json:"moduleParams"`
	Distance                bool                   `json:"distance"`
	Score                   bool                   `json:"score"`
	ExplainScore            bool                   `json:"explainScore"`
	IsConsistent            bool                   `json:"isConsistent"`
	Group                   bool                   `json:"group"`
	// QueryProfile enables per-shard query profiling data collection and return.
	QueryProfile bool `json:"queryProfile"`

	// The User is not interested in returning props, we can skip any costly
	// operation that isn't required.
	NoProps bool `json:"noProps"`

	// ReferenceQuery is used to indicate that a search
	// is being conducted on behalf of a referenced
	// property. for example: this is relevant when a
	// where filter operand is passed in with a path to
	// a referenced class, rather than a path to one of
	// its own props.
	//
	// The reason we need this indication is that
	// without it, the sub-Search which is
	// conducted to extract the reference propValuePair
	// is conducted with the pagination set to whatever
	// the QueryMaximumResults. if this value is set low
	// relative to the number of objects being searched,
	// weaviate will be unable to find enough results to
	// make any comparisons, and erroneously returns
	// empty, or with fewer results than expected.
	ReferenceQuery bool `json:"-"`
}

// Merge returns everything requested by either p or o, for a selection that
// was requested more than once. For a module param requested by both, p wins.
func (p Properties) Merge(o Properties) Properties {
	p.Classification = p.Classification || o.Classification
	p.RefMeta = p.RefMeta || o.RefMeta
	p.Vector = p.Vector || o.Vector
	p.IncludeAllTargetVectors = p.IncludeAllTargetVectors || o.IncludeAllTargetVectors
	p.Certainty = p.Certainty || o.Certainty
	p.ID = p.ID || o.ID
	p.CreationTimeUnix = p.CreationTimeUnix || o.CreationTimeUnix
	p.LastUpdateTimeUnix = p.LastUpdateTimeUnix || o.LastUpdateTimeUnix
	p.Distance = p.Distance || o.Distance
	p.Score = p.Score || o.Score
	p.ExplainScore = p.ExplainScore || o.ExplainScore
	p.IsConsistent = p.IsConsistent || o.IsConsistent
	p.Group = p.Group || o.Group
	p.QueryProfile = p.QueryProfile || o.QueryProfile
	p.ReferenceQuery = p.ReferenceQuery || o.ReferenceQuery
	// props are only skipped if neither selection wants them
	p.NoProps = p.NoProps && o.NoProps

	// Clip so appends never write into the backing array of p's caller
	p.Vectors = slices.Clip(p.Vectors)
	for _, vector := range o.Vectors {
		if !slices.Contains(p.Vectors, vector) {
			p.Vectors = append(p.Vectors, vector)
		}
	}

	if len(o.ModuleParams) > 0 {
		params := maps.Clone(o.ModuleParams)
		maps.Copy(params, p.ModuleParams)
		p.ModuleParams = params
	}
	return p
}
