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

package compactions

import (
	"math/rand"
	"strings"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/batch"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

const (
	ObjectsPerBatch = 200
	// 200 obj × 2 KB ≈ 400 KB/batch; keeps property_text growing
	TextSize = 2_000
)

// RandomText generates a string of approximately n ASCII characters composed
// of space-separated lowercase words, ensuring each object has unique tokens.
func RandomText(n int) string {
	var sb strings.Builder
	sb.Grow(n)
	words := []string{
		"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta",
		"theta", "iota", "kappa", "lambda", "mu", "nu", "xi", "omicron", "pi",
		"rho", "sigma", "tau", "upsilon", "phi", "chi", "psi", "omega",
	}
	for sb.Len() < n {
		w := words[rand.Intn(len(words))]
		sb.WriteString(w)
		sb.WriteByte(' ')
	}
	return sb.String()[:n]
}

func ImportBatch(t *testing.T, className string) {
	t.Helper()
	objects := make([]*models.Object, ObjectsPerBatch)
	for i := range objects {
		objects[i] = &models.Object{
			Class: className,
			ID:    strfmt.UUID(uuid.New().String()),
			Properties: map[string]interface{}{
				"text": RandomText(TextSize),
			},
		}
	}
	params := batch.NewBatchObjectsCreateParams().
		WithBody(batch.BatchObjectsCreateBody{Objects: objects})
	resp, err := helper.Client(t).Batch.BatchObjectsCreate(params, nil)
	require.NoError(t, err)
	require.NotNil(t, resp)
}
