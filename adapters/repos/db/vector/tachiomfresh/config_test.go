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

package tachiomfresh

import (
	"testing"

	"github.com/stretchr/testify/require"
	hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	ent "github.com/weaviate/weaviate/entities/vectorindex/tachiomfresh"
)

// TestValidateUserConfigUpdate: the two search budgets may change on a
// running index, nothing else may.
func TestValidateUserConfigUpdate(t *testing.T) {
	tests := []struct {
		name    string
		change  func(uc *ent.UserConfig)
		wantErr string
	}{
		{name: "no change", change: func(*ent.UserConfig) {}},
		{name: "kCentroids", change: func(uc *ent.UserConfig) { uc.KCentroids = 8 }},
		{name: "kDocsToScore", change: func(uc *ent.UserConfig) { uc.KDocsToScore = 50 }},
		{name: "distance", change: func(uc *ent.UserConfig) { uc.Distance = "cosine" }, wantErr: "distance is immutable"},
		{name: "maxPostingSize", change: func(uc *ent.UserConfig) { uc.MaxPostingSize = 128 }, wantErr: "maxPostingSize is immutable"},
		{name: "efConstruction", change: func(uc *ent.UserConfig) { uc.EFConstruction = 128 }, wantErr: "efConstruction is immutable"},
		{name: "multivector disabled", change: func(uc *ent.UserConfig) { uc.Multivector.Enabled = false }, wantErr: "multivector.enabled is immutable"},
		{name: "aggregation", change: func(uc *ent.UserConfig) { uc.Multivector.Aggregation = "other" }, wantErr: "multivector.aggregation is immutable"},
		{name: "muvera", change: func(uc *ent.UserConfig) { uc.Multivector.MuveraConfig.Enabled = true }, wantErr: "multivector.muvera.enabled is immutable"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			initial := ent.NewDefaultUserConfig()
			updated := ent.NewDefaultUserConfig()
			tt.change(&updated)
			err := ValidateUserConfigUpdate(initial, updated)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}

	t.Run("another index type's config", func(t *testing.T) {
		require.ErrorContains(t, ValidateUserConfigUpdate(hnswent.UserConfig{}, ent.NewDefaultUserConfig()), "initial is not UserConfig")
		require.ErrorContains(t, ValidateUserConfigUpdate(ent.NewDefaultUserConfig(), hnswent.UserConfig{}), "updated is not UserConfig")
	})
}

// TestUpdateUserConfig: the budgets land in the atomics the search reads,
// and another type's config is refused with the callback still called.
func TestUpdateUserConfig(t *testing.T) {
	tf := newTestIndex(t, 64, 8, 8, hfreshEFConstruction)
	uc := ent.NewDefaultUserConfig()
	uc.KCentroids = 3
	uc.KDocsToScore = 7
	var called int
	require.NoError(t, tf.Index.UpdateUserConfig(uc, func() { called++ }))
	require.Equal(t, 1, called)
	require.Equal(t, uint32(3), tf.Index.kCentroids)
	require.Equal(t, uint32(7), tf.Index.kDocsToScore)

	require.Error(t, tf.Index.UpdateUserConfig(hnswent.UserConfig{}, func() { called++ }))
	require.Equal(t, 2, called)
	require.Equal(t, uint32(3), tf.Index.kCentroids)
}
