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

package concepts

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
)

func TestHandlers(t *testing.T) {
	insp := newFakeInspector()
	h := NewRESTHandlers(insp)

	t.Run("without a concept", func(t *testing.T) {
		insp.reset()
		r := httptest.NewRequest("GET", "/", nil)
		w := httptest.NewRecorder()
		h.Handler().ServeHTTP(w, r)

		res := w.Result()
		defer res.Body.Close()
		assert.Equal(t, http.StatusNotFound, res.StatusCode)
	})

	t.Run("without any errors", func(t *testing.T) {
		insp.reset()
		r := httptest.NewRequest("GET", "/my-concept", nil)
		w := httptest.NewRecorder()
		h.Handler().ServeHTTP(w, r)

		res := w.Result()
		defer res.Body.Close()
		json, err := io.ReadAll(res.Body)
		require.Nil(t, err)
		expected := `{"individualWords":[{` +
			`"info":{"vector":[0.1,0.2]},"present":true,"word":"my-concept"}]}`

		assert.Equal(t, http.StatusOK, res.StatusCode)
		assert.Equal(t, expected, string(json))
	})

	t.Run("without an error from the UC", func(t *testing.T) {
		insp.reset()
		insp.err = errors.Errorf("invalid input")
		r := httptest.NewRequest("GET", "/my-concept", nil)
		w := httptest.NewRecorder()
		h.Handler().ServeHTTP(w, r)

		res := w.Result()
		defer res.Body.Close()
		json, err := io.ReadAll(res.Body)
		require.Nil(t, err)
		expected := `{"error":[{"message":"invalid input"}]}`

		assert.Equal(t, http.StatusBadRequest, res.StatusCode)
		assert.Equal(t, expected, string(json))
	})
}

type fakeInspector struct {
	err error
}

func (f *fakeInspector) reset() {
	f.err = nil
}

func (f *fakeInspector) GetWords(ctx context.Context,
	concept string,
) (*models.C11yWordsResponse, error) {
	return &models.C11yWordsResponse{
		IndividualWords: []*models.C11yWordsResponseIndividualWordsItems0{
			{
				Present: true,
				Word:    concept,
				Info: &models.C11yWordsResponseIndividualWordsItems0Info{
					Vector: []float32{0.1, 0.2},
				},
			},
		},
	}, f.err
}

func newFakeInspector() *fakeInspector {
	return &fakeInspector{}
}
