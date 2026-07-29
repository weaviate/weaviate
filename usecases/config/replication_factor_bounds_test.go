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

package config

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/replication"
)

func TestValidateReplicationFactorBounds(t *testing.T) {
	tests := []struct {
		name        string
		minFactor   int
		maxFactor   int
		wantErr     bool
		errContains string
	}{
		{name: "defaults (min=1, no cap)", minFactor: 1, maxFactor: 0, wantErr: false},
		{name: "min=1, max=1", minFactor: 1, maxFactor: 1, wantErr: false},
		{name: "min=3, max=3 (equal)", minFactor: 3, maxFactor: 3, wantErr: false},
		{name: "min=2, max=5 (within bounds)", minFactor: 2, maxFactor: 5, wantErr: false},
		{name: "min=3, no cap", minFactor: 3, maxFactor: 0, wantErr: false},
		{name: "min=3, negative max treated as no cap", minFactor: 3, maxFactor: -1, wantErr: false},
		{
			name: "min > max (the deadlock case)", minFactor: 3, maxFactor: 1,
			wantErr: true, errContains: "cannot exceed REPLICATION_MAXIMUM_FACTOR",
		},
		{
			name: "min=5, max=2", minFactor: 5, maxFactor: 2,
			wantErr: true, errContains: "cannot exceed REPLICATION_MAXIMUM_FACTOR",
		},
		{
			name: "min=0 (must be >= 1)", minFactor: 0, maxFactor: 0,
			wantErr: true, errContains: "REPLICATION_MINIMUM_FACTOR must be >= 1",
		},
		{
			name: "min=-1 (must be >= 1)", minFactor: -1, maxFactor: 3,
			wantErr: true, errContains: "REPLICATION_MINIMUM_FACTOR must be >= 1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Config{
				Replication: replication.GlobalConfig{
					MinimumFactor: tt.minFactor,
					MaximumFactor: tt.maxFactor,
				},
			}
			err := c.validateReplicationFactorBounds()
			if tt.wantErr {
				require.Error(t, err)
				assert.True(t, strings.Contains(err.Error(), tt.errContains),
					"error should contain %q, got: %v", tt.errContains, err)
				return
			}
			assert.NoError(t, err)
		})
	}
}
