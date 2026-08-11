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

package rest

import (
	"errors"
	"fmt"
	"testing"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
)

// What the operator reads after a cancel has to match what is left on disk. A
// sweep that skipped shards is an error and a later submit has to finish it. A
// sweep skipped because the collection is being deleted is neither: nothing is
// left, and the "next submit will retry" remedy names a submit that will never
// come for a collection that no longer exists.
func TestCancelCleanupSweepIsReportedAtTheLevelItsRemedyDeserves(t *testing.T) {
	strategies := []string{"searchable", "filterable"}

	tests := []struct {
		name            string
		errs            []error
		payloadReadable bool
		wantLevel       logrus.Level
		wantContains    []string
		wantNotContains string
	}{
		{
			name:            "a clean sweep says so",
			errs:            []error{nil, nil},
			payloadReadable: true,
			wantLevel:       logrus.InfoLevel,
			wantContains:    []string{"on-disk cleanup complete"},
		},
		{
			name:            "a clean sweep on an undecodable payload says which tuple it swept",
			errs:            []error{nil, nil},
			payloadReadable: false,
			wantLevel:       logrus.InfoLevel,
			wantContains:    []string{"the property and index type in the request URL"},
		},
		{
			name:            "a bounded per-shard failure names the strategies that failed",
			errs:            []error{errors.New("shard \"a\": disk full"), nil},
			payloadReadable: true,
			wantLevel:       logrus.ErrorLevel,
			wantContains:    []string{"cleaning partial reindex state on disk for 1 strategies failed"},
			wantNotContains: "not swept at all",
		},
		{
			name:            "a shutting-down node leaves shards unswept, and the next submit is the remedy",
			errs:            []error{fmt.Errorf("%w: node is shutting down", db.ErrCleanupSweepTruncated), nil},
			payloadReadable: true,
			wantLevel:       logrus.ErrorLevel,
			// The truncation phrase is the discriminating half: without it this
			// row asserts only the retry promise, which the bounded-failure arm
			// emits too, so dropping the truncated branch entirely stays green.
			wantContains: []string{
				"not swept at all",
				"next submit's defense-in-depth cleanup will retry",
			},
		},
		{
			name:            "a collection being deleted is not a failure and promises no retry",
			errs:            []error{fmt.Errorf("%w: collection is being deleted", db.ErrCleanupCollectionDropped), nil},
			payloadReadable: true,
			wantLevel:       logrus.InfoLevel,
			wantContains:    []string{"the collection is being deleted"},
			wantNotContains: "next submit",
		},
		{
			// The delete wins on one strategy and something else failed on the
			// other: the failure still has to reach the operator.
			name: "a real failure next to a deleted collection is still an error",
			errs: []error{
				fmt.Errorf("%w: collection is being deleted", db.ErrCleanupCollectionDropped),
				errors.New("shard \"a\": disk full"),
			},
			payloadReadable: true,
			wantLevel:       logrus.ErrorLevel,
			wantContains:    []string{"disk full"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Len(t, tc.errs, len(strategies), "one result per strategy swept")

			var outcome cleanupSweepOutcome
			for i, err := range tc.errs {
				outcome.add(strategies[i], err)
			}

			logger, hook := logrustest.NewNullLogger()
			logCleanupSweep(logger.WithField("taskID", "t-1"), strategies, outcome, tc.payloadReadable)

			require.Len(t, hook.AllEntries(), 1, "the sweep gets exactly one line, or the operator reads two verdicts")
			entry := hook.AllEntries()[0]
			require.Equalf(t, tc.wantLevel, entry.Level,
				"the level is the operator's cue to act; message was %q", entry.Message)
			for _, want := range tc.wantContains {
				require.Contains(t, entry.Message, want)
			}
			if tc.wantNotContains != "" {
				require.NotContains(t, entry.Message, tc.wantNotContains,
					"the message must not offer a remedy this outcome does not have")
			}
		})
	}
}

// The pre-submit sweep runs before every submit as defense in depth. Its
// failure means the new task may resume against stale state, which is worth
// waking someone for. A collection being deleted is not: there is no stale
// state left and the submit itself is about to be refused.
func TestPreSubmitCleanupSweepIsReportedAtTheLevelItsRemedyDeserves(t *testing.T) {
	tests := []struct {
		name         string
		err          error
		wantEntries  int
		wantLevel    logrus.Level
		wantContains string
	}{
		{
			name:        "a clean sweep is not worth a line",
			err:         nil,
			wantEntries: 0,
		},
		{
			name:         "a failed sweep warns that the new task may resume against stale state",
			err:          errors.New("shard \"a\": disk full"),
			wantEntries:  1,
			wantLevel:    logrus.ErrorLevel,
			wantContains: "operator inspection recommended",
		},
		{
			name:         "a collection being deleted has no stale state to leave behind",
			err:          fmt.Errorf("%w: collection is being deleted", db.ErrCleanupCollectionDropped),
			wantEntries:  1,
			wantLevel:    logrus.InfoLevel,
			wantContains: "the collection is being deleted",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger, hook := logrustest.NewNullLogger()
			logPreSubmitSweep(logger.WithField("index_type", "filterable"), tc.err)

			require.Len(t, hook.AllEntries(), tc.wantEntries)
			if tc.wantEntries == 0 {
				return
			}
			entry := hook.AllEntries()[0]
			require.Equalf(t, tc.wantLevel, entry.Level, "message was %q", entry.Message)
			require.Contains(t, entry.Message, tc.wantContains)
		})
	}
}
