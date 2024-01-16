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

package vectorizer

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/modules/ref2vec-centroid/config"
)

func TestVectorizer_New(t *testing.T) {
	repo := &fakeObjectsRepo{}
	t.Run("default is set correctly", func(t *testing.T) {
		vzr := New(fakeClassConfig(config.Default()), repo.Object)

		expected := reflect.ValueOf(calculateMean).Pointer()
		received := reflect.ValueOf(vzr.calcFn).Pointer()

		assert.EqualValues(t, expected, received)
	})

	t.Run("default calcFn is used when none provided", func(t *testing.T) {
		cfg := fakeClassConfig{"method": ""}
		vzr := New(cfg, repo.Object)

		expected := reflect.ValueOf(calculateMean).Pointer()
		received := reflect.ValueOf(vzr.calcFn).Pointer()

		assert.EqualValues(t, expected, received)
	})
}

func TestVectorizer_Object(t *testing.T) {
	t.Run("calculate with mean", func(t *testing.T) {
		type objectSearchResult struct {
			res *search.Result
			err error
		}

		tests := []struct {
			name                string
			objectSearchResults []objectSearchResult
			expectedResult      []float32
			expectedCalcError   error
		}{
			{
				name: "expected success 1",
				objectSearchResults: []objectSearchResult{
					{res: &search.Result{Vector: []float32{2, 4, 6}}},
					{res: &search.Result{Vector: []float32{4, 6, 8}}},
				},
				expectedResult: []float32{3, 5, 7},
			},
			{
				name: "expected success 2",
				objectSearchResults: []objectSearchResult{
					{res: &search.Result{Vector: []float32{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}}},
					{res: &search.Result{Vector: []float32{2, 2, 2, 2, 2, 2, 2, 2, 2, 2}}},
					{res: &search.Result{Vector: []float32{3, 3, 3, 3, 3, 3, 3, 3, 3, 3}}},
					{res: &search.Result{Vector: []float32{4, 4, 4, 4, 4, 4, 4, 4, 4, 4}}},
					{res: &search.Result{Vector: []float32{5, 5, 5, 5, 5, 5, 5, 5, 5, 5}}},
					{res: &search.Result{Vector: []float32{6, 6, 6, 6, 6, 6, 6, 6, 6, 6}}},
					{res: &search.Result{Vector: []float32{7, 7, 7, 7, 7, 7, 7, 7, 7, 7}}},
					{res: &search.Result{Vector: []float32{8, 8, 8, 8, 8, 8, 8, 8, 8, 8}}},
					{res: &search.Result{Vector: []float32{9, 9, 9, 9, 9, 9, 9, 9, 9, 9}}},
				},
				expectedResult: []float32{5, 5, 5, 5, 5, 5, 5, 5, 5, 5},
			},
			{
				name:                "expected success 3",
				objectSearchResults: []objectSearchResult{{}},
			},
			{
				name: "expected success 4",
				objectSearchResults: []objectSearchResult{
					{res: &search.Result{Vector: []float32{1, 2, 3, 4, 5, 6, 7, 8, 9}}},
				},
				expectedResult: []float32{1, 2, 3, 4, 5, 6, 7, 8, 9},
			},
			{
				name: "expected success 5",
				objectSearchResults: []objectSearchResult{
					{res: &search.Result{}},
				},
				expectedResult: nil,
			},
			{
				name: "expected error - mismatched vector dimensions",
				objectSearchResults: []objectSearchResult{
					{res: &search.Result{Vector: []float32{1, 2, 3, 4, 5, 6, 7, 8, 9}}},
					{res: &search.Result{Vector: []float32{1, 2, 3, 4, 5, 6, 7, 8}}},
				},
				expectedCalcError: errors.New(
					"calculate vector: calculate mean: found vectors of different length: 9 and 8"),
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				ctx := context.Background()
				repo := &fakeObjectsRepo{}
				refProps := []interface{}{"toRef"}
				cfg := fakeClassConfig{"method": "mean", "referenceProperties": refProps}

				crossRefs := make([]*crossref.Ref, len(test.objectSearchResults))
				modelRefs := make(models.MultipleRef, len(test.objectSearchResults))
				for i, res := range test.objectSearchResults {
					crossRef := crossref.New("localhost", "SomeClass",
						strfmt.UUID(uuid.NewString()))
					crossRefs[i] = crossRef
					modelRefs[i] = crossRef.SingleRef()

					repo.On("Object", ctx, crossRef.Class, crossRef.TargetID, "").
						Return(res.res, res.err)
				}

				obj := &models.Object{
					Properties: map[string]interface{}{"toRef": modelRefs},
				}

				err := New(cfg, repo.Object).Object(ctx, obj)
				if test.expectedCalcError != nil {
					assert.EqualError(t, err, test.expectedCalcError.Error())
				} else {
					assert.EqualValues(t, test.expectedResult, obj.Vector)
				}
			})
		}
	})

	// due to the fix introduced in https://github.com/weaviate/weaviate/pull/2320,
	// MultipleRef's can appear as empty []interface{} when no actual refs are provided for
	// an object's reference property.
	//
	// this test asserts that reference properties do not break when they are unmarshalled
	// as empty interface{} slices.
	t.Run("when rep prop is stored as empty interface{} slice", func(t *testing.T) {
		ctx := context.Background()
		repo := &fakeObjectsRepo{}
		refProps := []interface{}{"toRef"}
		cfg := fakeClassConfig{"method": "mean", "referenceProperties": refProps}

		obj := &models.Object{
			Properties: map[string]interface{}{"toRef": []interface{}{}},
		}

		err := New(cfg, repo.Object).Object(ctx, obj)
		assert.Nil(t, err)
		assert.Nil(t, obj.Vector)
	})
}

func TestVectorizer_Tenant(t *testing.T) {
	objectSearchResults := search.Result{Vector: []float32{}}
	ctx := context.Background()
	repo := &fakeObjectsRepo{}
	refProps := []interface{}{"toRef"}
	cfg := fakeClassConfig{"method": "mean", "referenceProperties": refProps}
	tenant := "randomTenant"

	crossRef := crossref.New("localhost", "SomeClass",
		strfmt.UUID(uuid.NewString()))
	modelRefs := models.MultipleRef{crossRef.SingleRef()}

	repo.On("Object", ctx, crossRef.Class, crossRef.TargetID, tenant).
		Return(&objectSearchResults, nil)

	obj := &models.Object{
		Properties: map[string]interface{}{"toRef": modelRefs},
		Tenant:     tenant,
	}

	err := New(cfg, repo.Object).Object(ctx, obj)
	assert.Nil(t, err)
	assert.Nil(t, obj.Vector)
}
