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

package nearText

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

func TestVectorForParamsKeepsVectorizerErrorChain(t *testing.T) {
	vectorizer := &fakeVectorizer{err: enterrors.NewErrModulePrecondition(errors.New("header X-Api-Key missing"))}
	vectorForParams := NewSearcher[[]float32](vectorizer).VectorSearches()["nearText"]

	_, err := vectorForParams(context.Background(), &NearTextParams{Values: []string{"space"}}, "Movie", nil, fakeClassConfig{})
	var precondition enterrors.ErrModulePrecondition
	require.ErrorAs(t, err, &precondition)
}
