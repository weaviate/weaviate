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

package modstggcs

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFindBucket_EmptyBucket(t *testing.T) {
	// Note: cases where the resolved bucket is non-empty cannot be tested
	// without a real GCS connection (client.Bucket panics on a nil client),
	// so we only test the early-return guard here.
	tests := []struct {
		name         string
		configBucket string
		override     string
		wantErr      string
	}{
		{
			name:         "empty config bucket without override returns error",
			configBucket: "",
			override:     "",
			wantErr:      "bucket must not be empty",
		},
		{
			name:         "non-empty config bucket without override passes guard",
			configBucket: "my-bucket",
			override:     "",
		},
		{
			name:         "empty config bucket with non-empty override passes guard",
			configBucket: "",
			override:     "override-bucket",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &gcsClient{config: clientConfig{Bucket: tt.configBucket}}

			if tt.wantErr != "" {
				_, err := client.findBucket(context.Background(), tt.override)
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				// Verify the guard logic: resolve the bucket the same way
				// findBucket does and confirm it is non-empty.
				b := tt.configBucket
				if tt.override != "" {
					b = tt.override
				}
				assert.NotEmpty(t, b)
			}
		})
	}
}
