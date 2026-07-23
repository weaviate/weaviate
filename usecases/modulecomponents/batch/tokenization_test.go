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

package batch

import (
	"errors"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/tiktoken-go"
)

func TestLoadEncoder(t *testing.T) {
	primary := &tiktoken.Tiktoken{}
	fallback := &tiktoken.Tiktoken{}
	loadErr := errors.New("download blocked")

	tests := []struct {
		name        string
		results     map[string]*tiktoken.Tiktoken // model -> encoder (absent = error)
		wantEncoder *tiktoken.Tiktoken
		wantWarn    bool
	}{
		{
			name:        "model has its own encoding",
			results:     map[string]*tiktoken.Tiktoken{"my-model": primary},
			wantEncoder: primary,
		},
		{
			name:        "falls back to ada-002",
			results:     map[string]*tiktoken.Tiktoken{"text-embedding-ada-002": fallback},
			wantEncoder: fallback,
		},
		{
			name:        "neither loads: nil + warning",
			results:     map[string]*tiktoken.Tiktoken{},
			wantEncoder: nil,
			wantWarn:    true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			load := func(model string) (*tiktoken.Tiktoken, error) {
				if tke, ok := tt.results[model]; ok {
					return tke, nil
				}
				return nil, loadErr
			}
			logger, hook := test.NewNullLogger()

			got := loadEncoder("my-model", load, logger)

			assert.Same(t, tt.wantEncoder, got)
			if tt.wantWarn {
				require.Len(t, hook.Entries, 1)
				assert.Equal(t, logrus.WarnLevel, hook.LastEntry().Level)
			} else {
				assert.Empty(t, hook.Entries)
			}
		})
	}
}
