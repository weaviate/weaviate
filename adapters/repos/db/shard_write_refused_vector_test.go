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

package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestRefusedVectorLeavesNoObjectBehind pins that a write reported as failed stores
// nothing. The index validates a vector before the LSM write, but it knows the
// dimensions only once it holds a vector.
func TestRefusedVectorLeavesNoObjectBehind(t *testing.T) {
	tests := []struct {
		name  string
		async bool
		skip  string
	}{
		{name: "sync indexing"},
		{
			name:  "async indexing, index still empty",
			async: true,
			skip:  "https://github.com/weaviate/weaviate/issues/13687",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.skip != "" {
				t.Skip(test.skip)
			}

			className := "RefusedVector"
			flavor := consistencyFlavors()[0]
			shard, _ := testShardWithSettings(t, t.Context(), consistencyClass(className),
				enthnsw.NewDefaultUserConfig(), false, test.async)

			objs := consistencyObjects(flavor, className, 2)
			objs[1].Vector = []float32{1, 2, 3}

			if !test.async {
				// the first vector has to be indexed for the index to validate the next
				require.Empty(t, nonNilErrs(shard.PutObjectBatch(t.Context(), objs[:1])))
				objs = objs[1:]
			}

			errs := shard.PutObjectBatch(t.Context(), objs)
			refused := objs[len(objs)-1]
			require.Error(t, errs[len(errs)-1])

			stored, err := shard.Exists(t.Context(), refused.ID())
			require.NoError(t, err)
			assert.False(t, stored, "a write reported as failed must not leave the object behind")
		})
	}
}
