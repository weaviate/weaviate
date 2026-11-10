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

package schema

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
)

// TestTachiomFreshBoundsOnSchemaWrites runs the config bounds through the
// schema handler, on create and on update of a live class.
func TestTachiomFreshBoundsOnSchemaWrites(t *testing.T) {
	class := func(field, value string) *models.Class {
		config := map[string]interface{}{}
		if field != "" {
			config[field] = json.Number(value)
		}
		return &models.Class{
			Class: "TachiomBounds",
			VectorConfig: map[string]models.VectorConfig{
				"mv": {
					VectorIndexType:   "tachiomfresh",
					Vectorizer:        map[string]interface{}{"none": nil},
					VectorIndexConfig: config,
				},
			},
		}
	}

	tests := []struct {
		name    string
		field   string
		value   string
		wantErr string
	}{
		{name: "maxPostingSize negative", field: "maxPostingSize", value: "-1", wantErr: "maxPostingSize is '-1'"},
		{name: "maxPostingSize above int64", field: "maxPostingSize", value: "9223372036854775808", wantErr: "maxPostingSize is '9223372036854775807'"},
		{name: "maxPostingSize at the maximum", field: "maxPostingSize", value: "100000"},
		{name: "maxPostingSize above the maximum", field: "maxPostingSize", value: "100001", wantErr: "must be at most 100000"},
		{name: "kCentroids negative", field: "kCentroids", value: "-1", wantErr: "kCentroids is '-1'"},
		{name: "kCentroids above int64", field: "kCentroids", value: "9223372036854775808", wantErr: "kCentroids is '9223372036854775807'"},
		{name: "kCentroids at the maximum", field: "kCentroids", value: "4096"},
		{name: "kCentroids above the maximum", field: "kCentroids", value: "4097", wantErr: "kCentroids is 4097 but must be at most 4096"},
		{name: "kCentroids above uint32", field: "kCentroids", value: "4294967296", wantErr: "kCentroids is '4294967296'"},
		{name: "kDocsToScore negative", field: "kDocsToScore", value: "-1", wantErr: "kDocsToScore is '-1'"},
		{name: "kDocsToScore above int64", field: "kDocsToScore", value: "9223372036854775808", wantErr: "kDocsToScore is '9223372036854775807'"},
		{name: "kDocsToScore at the maximum", field: "kDocsToScore", value: "20000"},
		{name: "kDocsToScore above the maximum", field: "kDocsToScore", value: "20001", wantErr: "kDocsToScore is 20001 but must be at most 20000"},
		{name: "kDocsToScore above uint32", field: "kDocsToScore", value: "4294967296", wantErr: "kDocsToScore is '4294967296'"},
		{name: "efConstruction at the maximum", field: "efConstruction", value: "1024"},
		{name: "efConstruction above the maximum", field: "efConstruction", value: "1025", wantErr: "efConstruction is 1025 but must be at most 1024"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, op := range []struct {
				name    string
				initial *models.Class
			}{
				{name: "create"},
				{name: "update", initial: class("", "")},
			} {
				t.Run(op.name, func(t *testing.T) {
					handler := newTestHandlerWithRealVectorConfigParser(t)
					err := handler.validateVectorSettingsAgainst(class(tt.field, tt.value), op.initial)
					if tt.wantErr != "" {
						require.ErrorContains(t, err, tt.wantErr)
						return
					}
					require.NoError(t, err)
				})
			}
		})
	}
}
