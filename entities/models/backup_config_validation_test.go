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

package models_test

import (
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
)

// TestBackupConfigDedupeConvergenceTimeoutBounds pins the spec's 1..600 gate, the only enforcement of the documented API maximum.
func TestBackupConfigDedupeConvergenceTimeoutBounds(t *testing.T) {
	tests := []struct {
		name    string
		seconds int64
		valid   bool
	}{
		{"unset means default", 0, true},
		{"minimum", 1, true},
		{"maximum", 600, true},
		{"above maximum", 601, false},
		{"negative", -1, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &models.BackupConfig{DedupeConvergenceTimeoutSeconds: tc.seconds}
			err := cfg.Validate(strfmt.Default)
			if tc.valid {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), "dedupeConvergenceTimeoutSeconds")
		})
	}
}
