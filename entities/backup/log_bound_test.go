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

package backup

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func shardRefusals(n int) []error {
	errs := make([]error, n)
	for i := range errs {
		errs[i] = fmt.Errorf(
			"node1/Cls: %w: shard %q (collection %q) has an active runtime-reindex task in DTM; retry after the migration finishes",
			ErrBackupBlockedByInFlightReindex, fmt.Sprintf("shard-%d", i), "Cls")
	}
	return errs
}

func TestErrorForLog(t *testing.T) {
	tests := []struct {
		name          string
		err           error
		wantUnchanged bool
		wantContains  []string
	}{
		{name: "nil"},
		{
			name:          "single refusal",
			err:           errors.Join(shardRefusals(1)...),
			wantUnchanged: true,
		},
		{
			name:          "at the line bound",
			err:           errors.Join(shardRefusals(logErrMaxLines)...),
			wantUnchanged: true,
		},
		{
			name:         "one line past the bound",
			err:          errors.Join(shardRefusals(logErrMaxLines + 1)...),
			wantContains: []string{"shard-0", "and 1 more of 6"},
		},
		{
			name:         "twenty thousand shards",
			err:          errors.Join(shardRefusals(20000)...),
			wantContains: []string{"shard-0", "and 19995 more of 20000"},
		},
		{
			name:         "one very long line",
			err:          errors.New(strings.Repeat("é", logErrMaxBytes)),
			wantContains: []string{"truncated"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ErrorForLog(tt.err)
			if tt.err == nil {
				require.NoError(t, got)
				return
			}
			require.Error(t, got)
			if tt.wantUnchanged {
				require.Equal(t, tt.err.Error(), got.Error())
				return
			}
			assert.LessOrEqual(t, len(got.Error()), logErrMaxBytes+64,
				"a log line must not grow with the number of shards")
			for _, want := range tt.wantContains {
				assert.Contains(t, got.Error(), want)
			}
			// The last shard is the part a full body would carry and a
			// bounded one must not.
			assert.NotEqual(t, tt.err.Error(), got.Error())
		})
	}
}

// TestErrorForLogKeepsValidUTF8 pins that the byte cap cuts on a rune
// boundary rather than splitting a multi-byte character.
func TestErrorForLogKeepsValidUTF8(t *testing.T) {
	got := ErrorForLog(errors.New(strings.Repeat("é", logErrMaxBytes)))
	require.True(t, strings.ToValidUTF8(got.Error(), "") == got.Error())
}
