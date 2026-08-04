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

package db

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	entitiesbackup "github.com/weaviate/weaviate/entities/backup"
)

// TestRefuseIfAnyReindexInFlight_Unwired pins the startup-window default: allow + warn once.
func TestRefuseIfAnyReindexInFlight_Unwired(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()
	db := &DB{logger: logger}

	require.NoError(t, db.RefuseIfAnyReindexInFlight(context.Background()))
	require.NoError(t, db.RefuseIfAnyReindexInFlight(context.Background()))

	warnings := 0
	for _, entry := range hook.AllEntries() {
		if entry.Level == logrus.WarnLevel &&
			strings.Contains(entry.Message, "AnyReindexActivityLookup not yet installed") {
			warnings++
		}
	}
	assert.Equal(t, 1, warnings,
		"the unwired WARN must fire once per process, not once per restore")
}

func TestRefuseIfAnyReindexInFlight(t *testing.T) {
	lookupErr := errors.New("DTM unreachable")

	tests := []struct {
		name         string
		lookup       AnyReindexActivityLookup
		cleanup      AnyCleanupInProgressLookup
		wantRefusal  bool
		wantContains string
		wantCause    error
	}{
		{
			name:   "no live task admits the restore",
			lookup: func(context.Context) (bool, error) { return false, nil },
		},
		{
			name:    "no live task and no cleanup admits the restore",
			lookup:  func(context.Context) (bool, error) { return false, nil },
			cleanup: func() bool { return false },
		},
		{
			name:         "sidecar cleanup after a cancel refuses the restore",
			lookup:       func(context.Context) (bool, error) { return false, nil },
			cleanup:      func() bool { return true },
			wantRefusal:  true,
			wantContains: "still removing its temporary index files",
		},
		{
			name:         "live task refuses the restore",
			lookup:       func(context.Context) (bool, error) { return true, nil },
			wantRefusal:  true,
			wantContains: "retry after the migration finishes",
		},
		{
			name:         "lookup failure fails closed",
			lookup:       func(context.Context) (bool, error) { return false, lookupErr },
			wantRefusal:  true,
			wantContains: "the cluster task manager could not be queried",
			wantCause:    lookupErr,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := logrustest.NewNullLogger()
			db := &DB{logger: logger}
			db.SetAnyReindexActivityLookup(tc.lookup)
			if tc.cleanup != nil {
				db.SetAnyCleanupInProgressLookup(tc.cleanup)
			}

			err := db.RefuseIfAnyReindexInFlight(context.Background())
			if !tc.wantRefusal {
				require.NoError(t, err)
				return
			}

			require.Error(t, err)
			require.ErrorIs(t, err, entitiesbackup.ErrReindexInFlight,
				"the refusal must carry the cluster-wide sentinel")
			assert.ErrorContains(t, err, tc.wantContains)
			if tc.wantCause != nil {
				assert.ErrorIs(t, err, tc.wantCause, "the underlying cause must stay reachable")
			}

			// The per-shard backup vocabulary doesn't apply to the cluster-wide gate.
			assert.NotContains(t, err.Error(), "backup")
			assert.NotContains(t, err.Error(), "restore")
			assert.NotContains(t, err.Error(), "this shard")
		})
	}
}

// TestRefuseIfAnyReindexInFlight_Wording pins the exact operator-facing text.
func TestRefuseIfAnyReindexInFlight_Wording(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	db := &DB{logger: logger}
	db.SetAnyReindexActivityLookup(func(context.Context) (bool, error) { return true, nil })

	err := db.RefuseIfAnyReindexInFlight(context.Background())
	require.Error(t, err)
	assert.Equal(t,
		`runtime-reindex in flight in the cluster: retry after the migration finishes `+
			`(poll GET /v1/schema/<class>/indexes until all indexes report status="ready") `+
			`or cancel it via PUT /v1/schema/<class>/indexes/<prop> {"<indexType>":{"cancel":true}}`,
		err.Error())
}

// TestRefuseIfAnyReindexInFlight_PropagatesContext pins that the caller's context reaches the lookup.
func TestRefuseIfAnyReindexInFlight_PropagatesContext(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	db := &DB{logger: logger}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	db.SetAnyReindexActivityLookup(func(ctx context.Context) (bool, error) {
		return false, ctx.Err()
	})

	err := db.RefuseIfAnyReindexInFlight(ctx)
	require.ErrorIs(t, err, entitiesbackup.ErrReindexInFlight)
	assert.ErrorIs(t, err, context.Canceled)
}

// The RAFT error behind a fail-closed refusal names the nodes it could not
// reach. Restoring grants nothing on node names, so that detail belongs in the
// log and not in the response body — the same rule the guard's 409 already
// follows.
func TestRefuseIfAnyReindexInFlight_LookupFailureRedactsNodeNames(t *testing.T) {
	raftErr := errors.New("can not resolve nodes [weaviate-2,weaviate-1]")

	logger, hook := logrustest.NewNullLogger()
	db := &DB{logger: logger}
	db.SetAnyReindexActivityLookup(func(context.Context) (bool, error) { return false, raftErr })

	err := db.RefuseIfAnyReindexInFlight(context.Background())
	require.Error(t, err)
	require.ErrorIs(t, err, entitiesbackup.ErrReindexInFlight)
	require.ErrorIs(t, err, raftErr,
		"the cause must stay reachable for callers that classify it")

	body := err.Error()
	assert.Equal(t,
		"runtime-reindex in flight in the cluster (assumed): "+
			"the cluster task manager could not be queried; retry once it is reachable",
		body)
	for _, leaked := range []string{"weaviate-1", "weaviate-2", "can not resolve nodes"} {
		assert.NotContainsf(t, body, leaked, "the refusal body leaked %q", leaked)
	}

	var logged bool
	for _, entry := range hook.AllEntries() {
		if strings.Contains(entry.Message, raftErr.Error()) {
			logged = true
		}
	}
	assert.True(t, logged, "the detail must still reach the operator through the log")
}
