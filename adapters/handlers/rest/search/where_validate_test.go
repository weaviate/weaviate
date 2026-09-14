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

package search

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWhereValidation: a bad path, operator or value is a client error naming
// what is wrong, not an untyped 500 from the engine.
func TestWhereValidation(t *testing.T) {
	tests := []struct {
		name       string
		nullState  bool
		where      string
		wantStatus int // 0: accepted
		want       string
	}{
		{
			name:  "reference path through a declared target is accepted",
			where: `{"path":["hasAuthor","Author","name"],"operator":"Equal","valueText":"Herbert"}`,
		},
		{
			name:       "unknown collection in a reference path is a 400, not a 404",
			where:      `{"path":["hasAuthor","NoSuchClass","name"],"operator":"Equal","valueText":"x"}`,
			wantStatus: http.StatusBadRequest,
			want:       `does not target collection "NoSuchClass"`,
		},
		{
			name:       "existing collection that is not a target of the reference is a 400",
			where:      `{"path":["hasAuthor","Studio","name"],"operator":"Equal","valueText":"x"}`,
			wantStatus: http.StatusBadRequest,
			want:       `does not target collection "Studio"`,
		},
		{
			name:       "path continuing through a non-reference property is a 400",
			where:      `{"path":["title","Author","name"],"operator":"Equal","valueText":"x"}`,
			wantStatus: http.StatusBadRequest,
			want:       "not a reference property",
		},
		{
			name:       "Not with two operands is a 400",
			where:      `{"operator":"Not","operands":[{"path":["year"],"operator":"Equal","valueInt":1},{"path":["year"],"operator":"Equal","valueInt":2}]}`,
			wantStatus: http.StatusBadRequest,
			want:       "exactly one operand",
		},
		{
			name:  "Not with one operand is accepted",
			where: `{"operator":"Not","operands":[{"path":["year"],"operator":"Equal","valueInt":1}]}`,
		},
		{
			name:       "ContainsAny with a scalar value is a 400",
			where:      `{"path":["tags"],"operator":"ContainsAny","valueText":"classic"}`,
			wantStatus: http.StatusBadRequest,
			want:       "needs an array value",
		},
		{
			name:  "ContainsAny with an array value is accepted",
			where: `{"path":["tags"],"operator":"ContainsAny","valueTextArray":["classic"]}`,
		},
		{
			name:       "Equal with an array value is a 400",
			where:      `{"path":["title"],"operator":"Equal","valueTextArray":["Dune"]}`,
			wantStatus: http.StatusBadRequest,
			want:       "takes a single value",
		},
		{
			name:       "malformed date is a 400",
			where:      `{"path":["published"],"operator":"GreaterThan","valueDate":"not-a-date"}`,
			wantStatus: http.StatusBadRequest,
			want:       "not an RFC3339 date",
		},
		{
			name:  "RFC3339 date is accepted",
			where: `{"path":["published"],"operator":"GreaterThan","valueDate":"1990-01-01T00:00:00Z"}`,
		},
		{
			name:       "empty Like pattern is a 400",
			where:      `{"path":["title"],"operator":"Like","valueText":" "}`,
			wantStatus: http.StatusBadRequest,
			want:       "non-empty pattern",
		},
		{
			name:       "IsNull without the null-state index is a 422",
			where:      `{"path":["title"],"operator":"IsNull","valueBoolean":true}`,
			wantStatus: http.StatusUnprocessableEntity,
			want:       "indexNullState",
		},
		{
			name:      "IsNull with the null-state index is accepted",
			nullState: true,
			where:     `{"path":["title"],"operator":"IsNull","valueBoolean":true}`,
		},
		{
			name:       "IsNull on a property without a filterable index is a 422",
			nullState:  true,
			where:      `{"path":["secret"],"operator":"IsNull","valueBoolean":true}`,
			wantStatus: http.StatusUnprocessableEntity,
			want:       "indexFilterable",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deps := newCatalogHandler(t, tt.nullState)
			_, apiErr := doBm25(t, deps, nil, "Catalog", fmt.Sprintf(`{"query":"dune","where":%s}`, tt.where))
			if tt.wantStatus == 0 {
				require.Nil(t, apiErr)
				return
			}
			require.NotNil(t, apiErr)
			assert.Equal(t, tt.wantStatus, apiErr.Status)
			assert.Contains(t, apiErr.Error(), tt.want)
		})
	}
}

// TestWhereUnknownRefClassOnEveryEndpoint: a bad reference path is 400 on
// every endpoint, never a 404 for a collection the caller never asked for.
func TestWhereUnknownRefClassOnEveryEndpoint(t *testing.T) {
	where := `"where":{"path":["hasAuthor","NoSuchClass","name"],"operator":"Equal","valueText":"x"}`
	run := map[string]func(*testDeps) *APIError{
		"near-text": func(d *testDeps) *APIError {
			_, e := doNearText(t, d, nil, "Movie", `{"query":["x"],`+where+`}`)
			return e
		},
		"bm25": func(d *testDeps) *APIError {
			_, e := doBm25(t, d, nil, "Movie", `{"query":"x",`+where+`}`)
			return e
		},
		"hybrid": func(d *testDeps) *APIError {
			_, e := doHybrid(t, d, nil, "Movie", `{"query":"x",`+where+`}`)
			return e
		},
		"near-object": func(d *testDeps) *APIError {
			_, e := doNearObject(t, d, nil, "Movie", `{"id":"11111111-2222-3333-4444-555555555555",`+where+`}`)
			return e
		},
		"aggregate": func(d *testDeps) *APIError {
			_, e := doAggregate(t, d, nil, "Movie", `{`+where+`}`)
			return e
		},
	}
	for name, fn := range run {
		t.Run(name, func(t *testing.T) {
			apiErr := fn(newTestHandler(t))
			require.NotNil(t, apiErr)
			assert.Equal(t, http.StatusBadRequest, apiErr.Status)
			assert.NotContains(t, apiErr.Error(), "could not find collection")
		})
	}
}
