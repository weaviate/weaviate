package vectorizer

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/modules/ref2vec-centroid/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestVectorizer_New(t *testing.T) {
	repo := &fakeRefVecRepo{}
	t.Run("default is set correctly", func(t *testing.T) {
		vzr := New(fakeClassConfig(config.Default()), repo.ReferenceVectorSearch)

		expected := reflect.ValueOf(calculateMean).Pointer()
		received := reflect.ValueOf(vzr.calcFunc).Pointer()

		assert.EqualValues(t, expected, received)
	})

	t.Run("default calcFunc is used when none provided", func(t *testing.T) {
		cfg := fakeClassConfig{"method": ""}
		vzr := New(cfg, repo.ReferenceVectorSearch)

		expected := reflect.ValueOf(calculateMean).Pointer()
		received := reflect.ValueOf(vzr.calcFunc).Pointer()

		assert.EqualValues(t, expected, received)
	})
}

func TestVectorizer_Object(t *testing.T) {
	t.Run("calculate with mean", func(t *testing.T) {
		tests := []struct {
			name              string
			refVecs           [][]float32
			expectedResult    []float32
			expectedCalcError error
			expectedSearchErr error
		}{
			{
				name: "expected success 1",
				refVecs: [][]float32{
					{2, 4, 6},
					{4, 6, 8},
				},
				expectedResult: []float32{3, 5, 7},
			},
			{
				name: "expected success 2",
				refVecs: [][]float32{
					{1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
					{2, 2, 2, 2, 2, 2, 2, 2, 2, 2},
					{3, 3, 3, 3, 3, 3, 3, 3, 3, 3},
					{4, 4, 4, 4, 4, 4, 4, 4, 4, 4},
					{5, 5, 5, 5, 5, 5, 5, 5, 5, 5},
					{6, 6, 6, 6, 6, 6, 6, 6, 6, 6},
					{7, 7, 7, 7, 7, 7, 7, 7, 7, 7},
					{8, 8, 8, 8, 8, 8, 8, 8, 8, 8},
					{9, 9, 9, 9, 9, 9, 9, 9, 9, 9},
				},
				expectedResult: []float32{5, 5, 5, 5, 5, 5, 5, 5, 5, 5},
			},
			{
				name:           "expected success 3",
				refVecs:        nil,
				expectedResult: nil,
			},
			{
				name: "expected success 4",
				refVecs: [][]float32{
					{1, 2, 3, 4, 5, 6, 7, 8, 9},
				},
				expectedResult: []float32{1, 2, 3, 4, 5, 6, 7, 8, 9},
			},
			{
				name:           "expected success 5",
				refVecs:        [][]float32{{}},
				expectedResult: nil,
			},
			{
				name: "expected error - mismatched vector dimensions",
				refVecs: [][]float32{
					{1, 2, 3, 4, 5, 6, 7, 8, 9},
					{1, 2, 3, 4, 5, 6, 7, 8},
				},
				expectedCalcError: errors.New(
					"calculate vector: calculate mean: found vectors of different length: 9 and 8"),
			},
			{
				name:              "expected error - failed ref vec search",
				expectedSearchErr: errors.New("not found"),
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				ctx := context.Background()
				repo := &fakeRefVecRepo{}
				refProps := []interface{}{"toRef"}
				cfg := fakeClassConfig{"method": "mean", "referenceProperties": refProps}
				vzr := New(cfg, repo.ReferenceVectorSearch)

				if test.expectedSearchErr != nil {
					repo.On("ReferenceVectorSearch", ctx, mock.Anything, mock.Anything).
						Return(nil, test.expectedSearchErr)
				} else {
					repo.On("ReferenceVectorSearch", ctx, mock.Anything, mock.Anything).
						Return(test.refVecs, nil)
				}

				obj := &models.Object{}
				err := vzr.Object(ctx, obj)
				if test.expectedCalcError != nil {
					assert.EqualError(t, err, test.expectedCalcError.Error())
				} else if test.expectedSearchErr != nil {
					expectedErr := fmt.Errorf("find ref vectors: %w", test.expectedSearchErr)
					assert.EqualError(t, err, expectedErr.Error())
				} else {
					assert.EqualValues(t, test.expectedResult, obj.Vector)
				}
			})
		}
	})
}
