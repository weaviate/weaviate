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
	"context"
	"errors"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/tiktoken-go"

	"github.com/weaviate/weaviate/entities/models"
	objectsvectorizer "github.com/weaviate/weaviate/usecases/modulecomponents/vectorizer"
)

func TestBatchTokenizerWhenTokenizerFailsToLoad(t *testing.T) {
	cfg := fakeClassConfig{classConfig: map[string]interface{}{
		"model":              "Snowflake/snowflake-arctic-embed-l-v2.0",
		"vectorizeClassName": false,
	}}
	objects := []*models.Object{{Class: "Car", Properties: map[string]interface{}{"text": "hello world"}}}
	const (
		estimatedCount = 3 + 3 // per-message tokens + len("hello world")/4 rounded up
		encodedCount   = 3 + 2 // per-message tokens + ["hello", " world"]
	)

	tests := []struct {
		name           string
		failedLoads    int // loads that fail before loading succeeds
		wantCounts     []int
		wantWarnings   int
		wantLoadsAfter int
	}{
		{
			name:           "vocabulary never loads",
			failedLoads:    4,
			wantCounts:     []int{estimatedCount, estimatedCount},
			wantWarnings:   2,
			wantLoadsAfter: 4,
		},
		{
			name:           "vocabulary loads on a later batch",
			failedLoads:    2,
			wantCounts:     []int{estimatedCount, encodedCount},
			wantWarnings:   1,
			wantLoadsAfter: 4,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, hook := test.NewNullLogger()
			cache := NewEncoderCache(logger)
			loads := 0
			cache.encodingForModel = func(model string) (*tiktoken.Tiktoken, error) {
				loads++
				if loads <= tt.failedLoads {
					return nil, errors.New("vocabulary unavailable")
				}
				return tiktoken.EncodingForModel(model)
			}
			tokenizer := ReturnBatchTokenizer(1, "", false)

			for _, wantCount := range tt.wantCounts {
				_, counts, _, _, err := tokenizer(context.Background(), objects, []bool{false}, cfg, objectsvectorizer.New(), cache)
				require.NoError(t, err)
				assert.Equal(t, []int{wantCount}, counts)
			}

			assert.Equal(t, tt.wantLoadsAfter, loads)
			require.Len(t, hook.AllEntries(), tt.wantWarnings)
			assert.Equal(t, logrus.WarnLevel, hook.LastEntry().Level)
		})
	}
}
