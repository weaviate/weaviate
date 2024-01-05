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

type Classification struct {
	BasedOn          []string        `json:"basedOn"`
	ClassifiedFields []string        `json:"classifiedFields"`
	Completed        strfmt.DateTime `json:"completed,omitempty"`
	ID               strfmt.UUID     `json:"id,omitempty"`
	Scope            []string        `json:"scope"`
}

type Properties struct {
	Classification     bool                   `json:"classification"`
	RefMeta            bool                   `json:"refMeta"`
	Vector             bool                   `json:"vector"`
	Certainty          bool                   `json:"certainty"`
	ID                 bool                   `json:"id"`
	CreationTimeUnix   bool                   `json:"creationTimeUnix"`
	LastUpdateTimeUnix bool                   `json:"lastUpdateTimeUnix"`
	ModuleParams       map[string]interface{} `json:"moduleParams"`
	Distance           bool                   `json:"distance"`
	Score              bool                   `json:"score"`
	ExplainScore       bool                   `json:"explainScore"`
	IsConsistent       bool                   `json:"isConsistent"`
	Group              bool                   `json:"group"`

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
