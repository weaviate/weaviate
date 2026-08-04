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

// TestRefuseIfAnyReindexInFlight_Unwired pins the startup-window default:
// allow the restore, and say so once rather than once per request. Kept in
// its own test because the warn-once state is process-wide.
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
		wantRefusal  bool
		wantContains string
		wantCause    error
	}{
		{
			name:   "no live task admits the restore",
			lookup: func(context.Context) (bool, error) { return false, nil },
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

			// The gate names the condition only. Any operation word here
			// would travel to seams that frame the refusal themselves, and
			// the per-shard backup vocabulary is wrong on both counts.
			assert.NotContains(t, err.Error(), "backup")
			assert.NotContains(t, err.Error(), "restore")
			assert.NotContains(t, err.Error(), "this shard")
		})
	}
}

// TestRefuseIfAnyReindexInFlight_Wording pins the exact operator-facing text,
// because both restore seams compose their message on top of it.
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

// TestRefuseIfAnyReindexInFlight_PropagatesContext pins that the caller's
// context reaches the lookup, so a canceled restore request does not leave a
// task-manager call running.
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
