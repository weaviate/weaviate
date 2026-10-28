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

package selfrecovery

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/namespaces"
)

func TestRunOne_NamespaceRefusalLogsInfo(t *testing.T) {
	const (
		waitStem  = "self-recovery waits for its namespace to become active"
		retryStem = "self-recovery register/poll failed; will retry"
	)
	tests := []struct {
		name      string
		refusal   error
		wantLevel logrus.Level
		wantStem  string
		otherStem string
	}{
		{"suspended", namespaces.ErrNamespaceSuspended, logrus.InfoLevel, waitStem, retryStem},
		{"resuming", namespaces.ErrNamespaceResuming, logrus.InfoLevel, waitStem, retryStem},
		{"deleting", namespaces.ErrNamespaceDeleting, logrus.WarnLevel, retryStem, waitStem},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := newOrchestratorWithDataPeer(t, &stubRaft{registerErr: fmt.Errorf("apply command: %w", tt.refusal)})
			logger, hook := logrustest.NewNullLogger()
			o.logger = logger

			o.runOne(context.Background(), ShardRef{Collection: "C", Shard: "S"}, false)

			entries := hook.AllEntries()
			require.True(t, slices.ContainsFunc(entries, func(e *logrus.Entry) bool {
				return e.Level == tt.wantLevel && strings.HasPrefix(e.Message, tt.wantStem)
			}), "no %s line %q", tt.wantLevel, tt.wantStem)
			require.False(t, slices.ContainsFunc(entries, func(e *logrus.Entry) bool {
				return strings.HasPrefix(e.Message, tt.otherStem)
			}), "logged %q", tt.otherStem)
			require.Equal(t, 1.0, testutil.ToFloat64(o.metrics.GiveupTotal), "a namespace refusal returns registerRetry, so runOne retries it to maxAttempts")
		})
	}
}
