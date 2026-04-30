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

package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
)

func TestValidateAnalyzerConfig(t *testing.T) {
	tests := []struct {
		name      string
		cfg       *models.TextAnalyzerConfig
		wantErr   bool
		errSubstr string
	}{
		{
			name: "nil config",
			cfg:  nil,
		},
		{
			name: "empty config",
			cfg:  &models.TextAnalyzerConfig{},
		},
		{
			name: "fold enabled no ignore",
			cfg:  &models.TextAnalyzerConfig{ASCIIFold: true},
		},
		{
			name: "fold enabled with valid ignore",
			cfg:  &models.TextAnalyzerConfig{ASCIIFold: true, ASCIIFoldIgnore: []string{"é", "ñ"}},
		},
		{
			name: "fold enabled with NFD single char",
			cfg:  &models.TextAnalyzerConfig{ASCIIFold: true, ASCIIFoldIgnore: []string{"é"}},
		},
		{
			name:      "ignore without fold",
			cfg:       &models.TextAnalyzerConfig{ASCIIFold: false, ASCIIFoldIgnore: []string{"é"}},
			wantErr:   true,
			errSubstr: "asciiFoldIgnore requires asciiFold",
		},
		{
			name:      "multi-character entry",
			cfg:       &models.TextAnalyzerConfig{ASCIIFold: true, ASCIIFoldIgnore: []string{"ab"}},
			wantErr:   true,
			errSubstr: "single character",
		},
		{
			name:      "empty string entry",
			cfg:       &models.TextAnalyzerConfig{ASCIIFold: true, ASCIIFoldIgnore: []string{""}},
			wantErr:   true,
			errSubstr: "single character",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateAnalyzerConfig(tt.cfg)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errSubstr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
