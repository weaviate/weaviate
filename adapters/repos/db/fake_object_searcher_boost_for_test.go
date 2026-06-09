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

	"github.com/weaviate/weaviate/entities/search"
)

// BoostValues completes the traverser.objectsSearcher interface for
// fakeObjectSearcher (hybrid_search_test.go). The boost feature added
// BoostValues to the interface (usecases/traverser/explorer.go) without
// updating this integrationTest-tagged fake, which broke compilation of the
// whole db package under the integrationTest build tag. Returning empty
// per-result maps makes boost scoring fall back to the results' materialized
// props, mirroring fakeVectorSearcher.BoostValues in
// usecases/traverser/fakes_for_test.go.
func (f *fakeObjectSearcher) BoostValues(_ context.Context, _, _ string,
	results []search.Result, _ []string,
) ([]map[string]any, error) {
	return make([]map[string]any, len(results)), nil
}
