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

package hnsw

import (
	"testing"

	"github.com/stretchr/testify/require"

	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestColumnarRescoreIsMutableOnUpdate pins that columnarRescore may be
// flipped via a vector index config update in both directions. This is
// deliberate and operationally safe: enabling triggers an idempotent
// backfill of the vector column; disabling stops feeding and serving it.
// Adding the field to ValidateUserConfigUpdate's immutable list would break
// the enable-on-running-cluster workflow.
func TestColumnarRescoreIsMutableOnUpdate(t *testing.T) {
	tests := []struct {
		name    string
		initial bool
		updated bool
	}{
		{name: "off to on", initial: false, updated: true},
		{name: "on to off", initial: true, updated: false},
		{name: "unchanged on", initial: true, updated: true},
		{name: "unchanged off", initial: false, updated: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			initial := ent.NewDefaultUserConfig()
			initial.ColumnarRescore = tt.initial
			updated := ent.NewDefaultUserConfig()
			updated.ColumnarRescore = tt.updated

			require.NoError(t, ValidateUserConfigUpdate(initial, updated))
		})
	}
}
