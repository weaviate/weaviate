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

package distributedtask

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

func TestUnreadablePayloadsLogsOneBoundedLine(t *testing.T) {
	longID := strings.Repeat("i", 300)
	longErr := errors.New(strings.Repeat("e", 300))
	for _, tc := range []struct {
		name      string
		ids       []string
		wantLines int
		wantIDs   int
	}{
		{name: "none"},
		{name: "many long", ids: []string{longID, longID, longID, longID, longID, longID, longID}, wantLines: 1, wantIDs: 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logger, hook := logrustest.NewNullLogger()
			var u UnreadablePayloads
			for _, id := range tc.ids {
				u.Add(id, longErr)
			}
			u.Log(logger, "things")

			require.Len(t, hook.AllEntries(), tc.wantLines)
			if tc.wantLines > 0 {
				msg := hook.LastEntry().Message
				require.True(t, strings.HasPrefix(msg, fmt.Sprintf("%d things; e.g. ", len(tc.ids))), msg)
				require.Equal(t, tc.wantIDs, strings.Count(msg, ", ")+1)
				require.LessOrEqual(t, len(msg), 5*200+4*len(", ")+200+50)
			}
		})
	}
}
