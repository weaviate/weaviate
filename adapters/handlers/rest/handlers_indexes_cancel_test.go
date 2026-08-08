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
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/entities/models"
)

// Every gate that fails open says so, and every one of those lines has to
// reach the handler's own logger — the sampler that rate-limits them is what
// carries it.
func TestEveryGateReportsWhenItFailsOpen(t *testing.T) {
	tests := []struct {
		name string
		// unwire removes one dependency from a handler that is otherwise whole.
		unwire func(h *indexesHandlers)
		// act defaults to a submission; the index-status read is the one gate
		// that fails open on a different route.
		act     func(t *testing.T, h *indexesHandlers)
		wantLog string
	}{
		{
			name:   "no task service, so the index status reads as ready",
			unwire: func(h *indexesHandlers) { h.tasks = nil },
			act: func(t *testing.T, h *indexesHandlers) {
				entry := getFilterableEntry(t, h)
				require.NotNil(t, entry, "the status read has to answer, not refuse")
				require.Equal(t, models.IndexStatusStatusReady, entry.Status,
					"the warn is only worth logging because the answer is the optimistic one")
			},
			wantLog: "distributed task service is not wired",
		},
		{
			name:    "no reindex provider, so the submit gate is a no-op",
			unwire:  func(h *indexesHandlers) { h.appState.ReindexProvider.Store(nil) },
			wantLog: "reindex provider is not wired",
		},
		{
			name:    "no backup activity probe, so no node is asked",
			unwire:  func(h *indexesHandlers) { h.backupActivity = nil },
			wantLog: "backup activity probe is not wired",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var busy atomic.Bool
			h := submissionHandlers(t, &raceTaskService{}, togglingProber{busy: &busy})
			logger, hook := logrustest.NewNullLogger()
			logger.SetLevel(logrus.DebugLevel)
			h.appState.Logger = logger
			h.appState.ReindexProvider.Store(db.NewReindexProvider(nil, nil, logger, fixtureNode,
				func() int { return 1 }, context.Background()))
			test.unwire(h)

			if test.act != nil {
				test.act(t, h)
			} else {
				require.IsType(t, &schema.SchemaObjectsIndexesUpdateAccepted{}, submitReindex(h),
					"the gate fails open, so the submission still goes through")
			}

			var found bool
			for _, entry := range hook.AllEntries() {
				if strings.Contains(entry.Message, test.wantLog) {
					found = true
					require.Equal(t, logrus.WarnLevel, entry.Level)
				}
			}
			require.Truef(t, found, "an ungated request has to be visible on the node's own logger")
		})
	}
}

// Each fail-open site gets its own hourly budget: one site spending its slot
// must not silence a different site on the same handler.
func TestGateWarnBudgetIsPerSite(t *testing.T) {
	var busy atomic.Bool
	h := submissionHandlers(t, &raceTaskService{}, togglingProber{busy: &busy})
	logger, hook := logrustest.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	h.appState.Logger = logger

	// Two fail-open sites on one submission: the submit gate and the backup probe.
	h.appState.ReindexProvider.Store(nil)
	h.backupActivity = nil

	const (
		submitWarn = "reindex provider is not wired"
		probeWarn  = "backup activity probe is not wired"
	)

	countWarns := func(want string) int {
		var n int
		for _, entry := range hook.AllEntries() {
			if strings.Contains(entry.Message, want) {
				require.Equal(t, logrus.WarnLevel, entry.Level)
				n++
			}
		}
		return n
	}

	require.IsType(t, &schema.SchemaObjectsIndexesUpdateAccepted{}, submitReindex(h),
		"both gates fail open, so the submission still goes through")
	require.Equal(t, 1, countWarns(submitWarn), "the submit gate spends its own slot")
	require.Equal(t, 1, countWarns(probeWarn),
		"the probe has its own budget, so the submit gate cannot have spent it")

	// A second submission is inside the same hour, so neither site logs again.
	submitReindex(h)
	require.Equal(t, 1, countWarns(submitWarn))
	require.Equal(t, 1, countWarns(probeWarn))
}
