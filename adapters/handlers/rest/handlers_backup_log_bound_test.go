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

// TestBackupRequestsTotal_LogErrorIsBounded pins that this logger no
// longer prints a refusal's whole body, which carries one line per
// blocked collection and so has no fixed size.
func TestBackupRequestsTotal_LogErrorIsBounded(t *testing.T) {
	// One line per blocked collection, which is what DB.Backupable joins.
	lines := make([]string, 20000)
	for i := range lines {
		lines[i] = fmt.Sprintf("%v: collection %q has an active runtime-reindex task in DTM",
			backup.ErrBackupBlockedByInFlightReindex, fmt.Sprintf("Cls-%d", i))
	}

	tests := []struct {
		name string
		err  error
	}{
		{name: "genuine reindex blocking every collection", err: errors.New(strings.Join(lines, "\n"))},
		{
			name: "cluster leader unreachable",
			err: fmt.Errorf("%w: the cluster leader could not be reached, so runtime-reindex state is unknown",
				backup.ErrBackupBlockedByInFlightReindex),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, hook := test.NewNullLogger()
			e := newBackupRequestsTotal(nil, logger)

			e.logError("Cls", tt.err)

			var sawRefusal bool
			for _, entry := range hook.AllEntries() {
				loggedErr, ok := entry.Data["error"]
				if !ok {
					continue
				}
				sawRefusal = true
				logged := entry.Message + fmt.Sprint(loggedErr)
				require.LessOrEqual(t, len(logged), 8<<10,
					"log line must not grow with the number of refused collections")
			}
			require.True(t, sawRefusal,
				"a refused backup must reach the log with its error attached; "+
					"a bound proves nothing about a line nobody writes")
		})
	}
}
