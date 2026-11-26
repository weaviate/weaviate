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

//go:build integrationTest

package db

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/entities/search"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/objects"
)

const (
	refFilterSource = "RefFilterSource"
	refFilterTarget = "RefFilterTarget"
	refFilterIDA    = strfmt.UUID("a0000000-0000-0000-0000-000000000001")
	refFilterIDB    = strfmt.UUID("b0000000-0000-0000-0000-000000000001")
)

// Test_RefCountFilter_AfterReferenceChurn pins weaviate/weaviate#6701: after a
// reference is deleted and then set again, the object must match exactly one
// of "ref count = 0" and "ref count > 0". Deleting a reference writes the
// object back with an empty reference list, which storage returns as []any;
// the previous version's count of 0 must still be removed by the next update,
// whichever write path brings the reference back.
func Test_RefCountFilter_AfterReferenceChurn(t *testing.T) {
	type step string
	const (
		insertWithRef step = "insert with ref"
		addRef        step = "add ref"
		batchAddRef   step = "batch add ref"
		putWithRef    step = "put with ref"
		deleteRef     step = "delete ref"
		patchOther    step = "patch other prop"
	)
	cases := []struct {
		steps    []step
		wantRefs int
	}{
		{steps: []step{insertWithRef}, wantRefs: 1},
		{steps: []step{addRef}, wantRefs: 1},
		{steps: []step{addRef, deleteRef}, wantRefs: 0},
		// the reported sequence
		{steps: []step{addRef, deleteRef, addRef}, wantRefs: 1},
		{steps: []step{insertWithRef, deleteRef, addRef}, wantRefs: 1},
		{steps: []step{addRef, deleteRef, batchAddRef}, wantRefs: 1},
		{steps: []step{addRef, deleteRef, putWithRef}, wantRefs: 1},
		{steps: []step{addRef, deleteRef, patchOther}, wantRefs: 0},
		{steps: []step{addRef, deleteRef, patchOther, addRef}, wantRefs: 1},
		{steps: []step{addRef, deleteRef, addRef, deleteRef}, wantRefs: 0},
		{steps: []step{addRef, deleteRef, addRef, deleteRef, addRef}, wantRefs: 1},
	}

	for _, tc := range cases {
		name := fmt.Sprint(tc.steps)
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			repo := createTestDatabaseWithClass(t, monitoring.GetMetrics(), refFilterTestClasses()...)
			beacon := func() *models.SingleRef {
				return crossref.New("localhost", refFilterTarget, refFilterIDB).SingleRef()
			}
			source, err := crossref.ParseSource(fmt.Sprintf("weaviate://localhost/%s/%s/toTarget", refFilterSource, refFilterIDA))
			require.NoError(t, err)

			require.NoError(t, repo.PutObject(ctx, &models.Object{
				Class: refFilterTarget, ID: refFilterIDB,
				Properties: map[string]interface{}{"name": "b"},
			}, []float32{1, 0}, nil, nil, nil, 0))

			props := map[string]interface{}{"name": "a"}
			if tc.steps[0] == insertWithRef {
				props["toTarget"] = models.MultipleRef{beacon()}
			}
			require.NoError(t, repo.PutObject(ctx, &models.Object{
				Class: refFilterSource, ID: refFilterIDA, Properties: props,
			}, []float32{1, 0}, nil, nil, nil, 0))

			for _, st := range tc.steps {
				switch st {
				case insertWithRef:
				case addRef:
					require.NoError(t, repo.AddReference(ctx, source, crossref.New("localhost", refFilterTarget, refFilterIDB), nil, "", 0))
				case batchAddRef:
					res, err := repo.AddBatchReferences(ctx, objects.BatchReferences{{
						From: source, To: crossref.New("localhost", refFilterTarget, refFilterIDB),
					}}, nil, 0)
					require.NoError(t, err)
					require.NoError(t, res[0].Err)
				case putWithRef, deleteRef:
					// a delete is what objects.Manager.DeleteObjectReference
					// does: read the object, drop the beacon, write it back
					res, err := repo.ObjectByID(ctx, refFilterIDA, search.SelectProperties{}, additional.Properties{}, "")
					require.NoError(t, err)
					require.NotNil(t, res)
					obj := res.Object()
					refs := models.MultipleRef{}
					if st == putWithRef {
						refs = models.MultipleRef{beacon()}
					}
					obj.Properties.(map[string]interface{})["toTarget"] = refs
					require.NoError(t, repo.PutObject(ctx, obj, res.Vector, nil, nil, nil, 0))
				case patchOther:
					require.NoError(t, repo.Merge(ctx, objects.MergeDocument{
						Class: refFilterSource, ID: refFilterIDA,
						PrimitiveSchema: map[string]interface{}{"name": "a2"},
					}, nil, "", 0))
				}
			}

			res, err := repo.ObjectByID(ctx, refFilterIDA, search.SelectProperties{{Name: "toTarget"}}, additional.Properties{}, "")
			require.NoError(t, err)
			require.NotNil(t, res)
			storedRefs, _ := res.Object().Properties.(map[string]interface{})["toTarget"].(models.MultipleRef)
			require.Len(t, storedRefs, tc.wantRefs, "stored references")

			wantZero, wantPositive := 0, 0
			if tc.wantRefs == 0 {
				wantZero = 1
			} else {
				wantPositive = 1
			}
			require.Equal(t, wantZero, refCountMatches(t, repo, filters.OperatorEqual, 0), "objects matching ref count = 0")
			require.Equal(t, wantPositive, refCountMatches(t, repo, filters.OperatorGreaterThan, 0), "objects matching ref count > 0")
			require.Equal(t, 1, refCountMatches(t, repo, filters.OperatorEqual, tc.wantRefs),
				"objects matching ref count = %d (the stored count)", tc.wantRefs)
		})
	}
}

func refCountMatches(t *testing.T, repo *DB, op filters.Operator, count int) int {
	t.Helper()
	res, err := repo.Search(context.Background(), dto.GetParams{
		ClassName: refFilterSource,
		Filters: &filters.LocalFilter{Root: &filters.Clause{
			Operator: op,
			On:       &filters.Path{Class: refFilterSource, Property: "toTarget"},
			Value:    &filters.Value{Value: count, Type: schema.DataTypeInt},
		}},
		Pagination: &filters.Pagination{Limit: 10},
	})
	require.NoError(t, err)
	return len(res)
}

func refFilterTestClasses() []*models.Class {
	return []*models.Class{
		{
			Class:               refFilterTarget,
			VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
			InvertedIndexConfig: invertedConfig(),
			Properties: []*models.Property{
				{Name: "name", DataType: schema.DataTypeText.PropString(), Tokenization: models.PropertyTokenizationWhitespace},
			},
		},
		{
			Class:               refFilterSource,
			VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
			InvertedIndexConfig: invertedConfig(),
			Properties: []*models.Property{
				{Name: "name", DataType: schema.DataTypeText.PropString(), Tokenization: models.PropertyTokenizationWhitespace},
				{Name: "toTarget", DataType: []string{refFilterTarget}},
			},
		},
	}
}
