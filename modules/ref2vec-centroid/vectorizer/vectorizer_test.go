package vectorizer

import (
	"errors"
	"reflect"
	"testing"

	"github.com/semi-technologies/weaviate/modules/ref2vec-centroid/config"
	"github.com/stretchr/testify/assert"
)

func TestVectorizer(t *testing.T) {
	repo := &fakeRefVecRepo{}
	t.Run("default is set correctly", func(t *testing.T) {
		vzr := New(fakeClassConfig(config.Default()), repo.ReferenceVectorSearch)

		expected := reflect.ValueOf(calculateMean).Pointer()
		received := reflect.ValueOf(vzr.calcFunc).Pointer()

		assert.EqualValues(t, expected, received)
	})

	t.Run("no calcFunc set", func(t *testing.T) {
		vzr := &Vectorizer{}

		expectedErr := "vectorizer calcFunc not set"
		_, err := vzr.calculateVector([]float32{1, 2, 3})
		assert.EqualError(t, err, expectedErr)
	})

	t.Run("calculate with mean", func(t *testing.T) {
		tests := []struct {
			name           string
			refVecs        [][]float32
			expectedResult []float32
			expectedError  error
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
				expectedError: errors.New("calculate mean: found vectors of different length: 9 and 8"),
			},
		}

		cfg := fakeClassConfig{"method": "mean"}
		vzr := New(cfg, repo.ReferenceVectorSearch)

		for _, test := range tests {
			res, err := vzr.calculateVector(test.refVecs...)
			if test.expectedError != nil {
				assert.EqualError(t, err, test.expectedError.Error())
			} else {
				assert.Equal(t, test.expectedResult, res)
			}
		}
	})
}
