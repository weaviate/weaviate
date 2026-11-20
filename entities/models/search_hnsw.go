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

package models

import (
	"github.com/go-openapi/errors"
	"github.com/go-openapi/strfmt"
)

// SearchHNSW is the HNSW-only portion of a REST search request.
// It is shared by every search type because any of them can run a vector search.
//
// swagger:model SearchHNSW
type SearchHNSW struct {
	// Size of the HNSW candidate list for this query. Overrides the collection's
	// vectorIndexConfig.ef, including dynamic ef. Must be between 1 and 1048576
	// (searchparams.MaxEF).
	EF *int64 `json:"ef,omitempty"`
}

// searchHNSWMaxEF matches searchparams.MaxEF. models cannot import searchparams
// (that package already imports models).
const searchHNSWMaxEF int64 = 1 << 20

// Validate validates this search HNSW override.
func (m *SearchHNSW) Validate(formats strfmt.Registry) error {
	// 0 is invalid, so this cannot use swag.IsZero: an explicit 0 must be rejected.
	if m == nil || m.EF == nil {
		return nil
	}
	if *m.EF < 1 || *m.EF > searchHNSWMaxEF {
		return errors.New(422, "hnsw.ef must be between 1 and 1048576")
	}
	return nil
}

func (m *SearchCommon) validateHNSW(formats strfmt.Registry) error {
	if m == nil || m.HNSW == nil {
		return nil
	}
	return m.HNSW.Validate(formats)
}
