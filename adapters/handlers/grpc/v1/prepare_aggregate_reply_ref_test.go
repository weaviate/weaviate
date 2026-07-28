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

package v1

import (
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/aggregation"
)

// A grouped-by value that is still a strfmt.URI must render as text rather than
// reaching the default arm, which returns an error and surfaces as a 500.
func TestParseAggregateGroupedByAcceptsURI(t *testing.T) {
	const beacon = "weaviate://localhost/SomeClass/6c2f5a1e-0000-4000-8000-000000000001"
	r := &AggregateReplier{}

	got, err := r.parseAggregateGroupedBy(&aggregation.GroupedBy{
		Path:  []string{"refProp"},
		Value: strfmt.URI(beacon),
	})
	require.NoError(t, err)
	require.Equal(t, beacon, got.GetText())
}
