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

package objects

import (
	"context"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	tachiomfreshent "github.com/weaviate/weaviate/entities/vectorindex/tachiomfresh"
)

// Test_MergeObjectTokenIDs: a patch passes its token ids on to the merge
// whether or not it changes the vectors, and they must give one id per
// token of the multi-vector the merged object holds.
func Test_MergeObjectTokenIDs(t *testing.T) {
	const (
		cls    = "Tachiom"
		target = "colbert"
	)
	uuid := strfmt.UUID("dd59815b-142b-4c54-9b12-482434bd54ca")
	tokens := [][]float32{{1, 2}, {3, 4}}
	sch := schema.Schema{Objects: &models.Schema{Classes: []*models.Class{{
		Class:      cls,
		Properties: []*models.Property{{Name: "name", DataType: schema.DataTypeText.PropString()}},
		VectorConfig: map[string]models.VectorConfig{target: {
			VectorIndexType:   "tachiomfresh",
			VectorIndexConfig: tachiomfreshent.NewDefaultUserConfig(),
			Vectorizer:        map[string]interface{}{"none": map[string]interface{}{}},
		}},
	}}}}

	tests := []struct {
		name    string
		updated *models.Object
		// noPreviousVectors: the stored object holds no multi-vector
		noPreviousVectors bool
		wantCode          int
		want              *MergeDocument
	}{
		{
			name: "token ids alone",
			updated: &models.Object{
				Class: cls, ID: uuid,
				TokenIds: models.TokenIds{target: {7, 9}},
			},
			want: &MergeDocument{
				Class: cls, ID: uuid, UpdateTime: 12345,
				PrimitiveSchema: map[string]interface{}{},
				TokenIds:        models.TokenIds{target: {7, 9}},
			},
		},
		{
			name: "token ids alone, one per token missing",
			updated: &models.Object{
				Class: cls, ID: uuid,
				TokenIds: models.TokenIds{target: {7}},
			},
			wantCode: StatusBadRequest,
		},
		{
			name:              "token ids of a multi-vector the object does not hold",
			noPreviousVectors: true,
			updated: &models.Object{
				Class: cls, ID: uuid,
				TokenIds: models.TokenIds{target: {7, 9}},
			},
			wantCode: StatusBadRequest,
		},
		{
			name: "the same vectors and new token ids",
			updated: &models.Object{
				Class: cls, ID: uuid,
				Vectors:  models.Vectors{target: tokens},
				TokenIds: models.TokenIds{target: {7, 9}},
			},
			want: &MergeDocument{
				Class: cls, ID: uuid, UpdateTime: 12345,
				PrimitiveSchema: map[string]interface{}{},
				TokenIds:        models.TokenIds{target: {7, 9}},
			},
		},
		{
			name: "new vectors and their token ids",
			updated: &models.Object{
				Class: cls, ID: uuid,
				Vectors:  models.Vectors{target: [][]float32{{5, 6}, {7, 8}, {9, 10}}},
				TokenIds: models.TokenIds{target: {1, 2, 3}},
			},
			want: &MergeDocument{
				Class: cls, ID: uuid, UpdateTime: 12345,
				PrimitiveSchema: map[string]interface{}{},
				Vectors:         models.Vectors{target: [][]float32{{5, 6}, {7, 8}, {9, 10}}},
				TokenIds:        models.TokenIds{target: {1, 2, 3}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newFakeGetManager(sch)
			m.timeSource = fakeTimeSource{}
			previous := &search.Result{ClassName: cls, Schema: map[string]interface{}{"name": "a"}}
			if !tt.noPreviousVectors {
				previous.Vectors = models.Vectors{target: tokens}
			}
			m.repo.On("Object", cls, uuid, search.SelectProperties(nil), additional.Properties{}, "").Return(previous, nil)
			m.modulesProvider.On("UpdateVector", mock.Anything, mock.Anything).Maybe().Return(nil, nil)
			if tt.want != nil {
				m.repo.On("Merge", *tt.want).Return(nil)
			}

			err := m.MergeObject(context.Background(), nil, tt.updated, nil)
			if tt.wantCode != 0 {
				require.NotNil(t, err)
				require.Equal(t, tt.wantCode, err.Code, "%v", err)
				m.repo.AssertNotCalled(t, "Merge", mock.Anything)
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			m.repo.AssertExpectations(t)
		})
	}
}
