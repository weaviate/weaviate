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

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
)

// TestCompareObjsForInsertStatusTokenIDs: token ids go with the
// multi-vectors. An update that changes them changes what the vector index
// must hold, so it neither skips the update nor keeps the doc id.
func TestCompareObjsForInsertStatusTokenIDs(t *testing.T) {
	multi := map[string][][]float32{"colbert": {{1, 2}, {3, 4}}}
	object := func(n int64, tokenIDs models.TokenIds) *storobj.Object {
		return &storobj.Object{
			Object:       models.Object{Properties: map[string]interface{}{"n": n}, TokenIds: tokenIDs},
			MultiVectors: multi,
		}
	}
	ids := models.TokenIds{"colbert": {7, 8}}
	tests := []struct {
		name                   string
		prev, next             *storobj.Object
		wantPreserve, wantSkip bool
	}{
		{name: "nothing changed", prev: object(1, ids), next: object(1, ids), wantSkip: true},
		{name: "a property changed", prev: object(1, ids), next: object(2, ids), wantPreserve: true},
		{name: "the token ids changed", prev: object(1, ids), next: object(1, models.TokenIds{"colbert": {7, 9}})},
		{name: "the token ids and a property changed", prev: object(1, ids), next: object(2, models.TokenIds{"colbert": {7, 9}})},
		{name: "token ids added", prev: object(1, nil), next: object(1, ids)},
		{name: "token ids removed", prev: object(1, ids), next: object(1, nil)},
		{name: "token ids of another target", prev: object(1, ids), next: object(1, models.TokenIds{"colbert": {7, 8}, "other": {1}})},
		{name: "no token ids, empty or nil", prev: object(1, nil), next: object(1, models.TokenIds{}), wantSkip: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			preserve, skip := compareObjsForInsertStatus(tt.prev, tt.next)
			require.Equal(t, tt.wantPreserve, preserve, "preserve")
			require.Equal(t, tt.wantSkip, skip, "skip")
		})
	}
}
