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

	ent "github.com/weaviate/weaviate/entities/vectorindex/tachiomfresh"
)

// A live update through the migrator's dispatch changes the two search budgets
// and refuses what would need a rebuild.
func TestMigratorValidatesTachiomFreshUpdate(t *testing.T) {
	tests := []struct {
		name    string
		change  func(uc *ent.UserConfig)
		wantErr string
	}{
		{name: "kCentroids", change: func(uc *ent.UserConfig) { uc.KCentroids = 8 }},
		{name: "kDocsToScore", change: func(uc *ent.UserConfig) { uc.KDocsToScore = 50 }},
		{name: "maxPostingSize", change: func(uc *ent.UserConfig) { uc.MaxPostingSize = 128 }, wantErr: "maxPostingSize is immutable"},
		{name: "distance", change: func(uc *ent.UserConfig) { uc.Distance = "cosine" }, wantErr: "distance is immutable"},
		{name: "efConstruction", change: func(uc *ent.UserConfig) { uc.EFConstruction = 128 }, wantErr: "efConstruction is immutable"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			updated := ent.NewDefaultUserConfig()
			tt.change(&updated)
			err := (&Migrator{}).ValidateVectorIndexConfigUpdate(ent.NewDefaultUserConfig(), updated)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}
