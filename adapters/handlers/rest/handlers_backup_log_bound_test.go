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
	"strings"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
)

// TestBackupRequestsTotal_LogErrorIsBounded pins that this logger no longer
// prints a server error's whole body. Backup errors are joined per failing
// unit, so a wide failure has no fixed size.
//
// The inputs are the ones that actually arrive here: the gate refusals reach
// this method wrapped in backup.ErrUnprocessable (usecases/backup/scheduler.go)
// and the arm above sends those to logUserError, which writes no line at all.
func TestBackupRequestsTotal_LogErrorIsBounded(t *testing.T) {
	// One line per object the backend could not read, which is what a
	// storage-backend failure over a wide collection joins.
	lines := make([]string, 20000)
	for i := range lines {
		lines[i] = fmt.Sprintf("upload %q: connection reset by peer", fmt.Sprintf("obj-%d", i))
	}

	tests := []struct {
		name string
		err  error
	}{
		{name: "storage backend names every object it failed on", err: errors.New(strings.Join(lines, "\n"))},
		{name: "one line carrying the whole failure", err: errors.New("backend s3: " + strings.Repeat("x", 100<<10))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, hook := test.NewNullLogger()
			e := newBackupRequestsTotal(nil, logger)

			e.logError("Cls", tt.err)

			var sawFailure bool
			for _, entry := range hook.AllEntries() {
				loggedErr, ok := entry.Data["error"]
				if !ok {
					continue
				}
				sawFailure = true
				logged := entry.Message + fmt.Sprint(loggedErr)
				require.LessOrEqual(t, len(logged), 8<<10,
					"log line must not grow with the number of failed units")
			}
			require.True(t, sawFailure,
				"a failed backup must reach the log with its error attached; "+
					"a bound proves nothing about a line nobody writes")
		})
	}
}

// TestBackupRequestsTotal_RefusalsAreNotServerErrors pins the routing the bound
// above depends on: a reindex refusal arrives wrapped the way
// Scheduler.Backup wraps it, and that shape is a user error, so it is counted
// and not logged.
func TestBackupRequestsTotal_RefusalsAreNotServerErrors(t *testing.T) {
	logger, hook := test.NewNullLogger()
	e := newBackupRequestsTotal(nil, logger)

	e.logError("Cls", backup.NewErrUnprocessable(fmt.Errorf(
		"%w: collection %q has an active runtime-reindex task in DTM",
		backup.ErrBackupBlockedByInFlightReindex, "Cls")))

	for _, entry := range hook.AllEntries() {
		require.NotContains(t, entry.Data, "error",
			"a retryable refusal is the caller's to read from the response, not a server-side failure to log")
	}
}
