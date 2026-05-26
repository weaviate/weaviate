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

//go:build integrationTest

package db

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
)

// TestDB_IndexLock_BlastRadius_Synthetic answers the question:
// "If a single slow operation holds db.indexLock.Lock(), what else is blocked?"
//
// Context: weaviate/0-weaviate-issues#249 / #250 — a single TYPE_DELETE_CLASS
// raft apply holds db.indexLock for ~3 minutes on a busy follower. The static
// call-site sweep (Lock vs RLock) shows that every CRUD/search/batch path
// takes db.indexLock.RLock() before any class-specific work. This test
// proves the dynamic consequence: while a writer holds Lock, all readers
// (data writes/reads/searches) and every other writer (schema mutations on
// unrelated classes) are gated.
//
// Mechanism (the test does NOT simulate the slow DELETE; it directly grabs
// the lock to isolate the indexLock blast radius from any other timing). A
// companion test that injects a sleep into stopCycleManagers and triggers
// DeleteIndex would prove the production path; this test proves the lock's
// scope independently of the trigger.
func TestDB_IndexLock_BlastRadius_Synthetic(t *testing.T) {
	ctx := context.Background()
	rootDir := t.TempDir()
	classB := makeTestClass("BlastRadiusClassB")
	db := setupTestDB(t, rootDir, classB)
	defer db.Shutdown(ctx)

	migrator := NewMigrator(db, db.logger, "node1")

	// Seed B with one object so reads have something to look up.
	seedID := strfmt.UUID("00000000-0000-0000-0000-000000000001")
	require.NoError(t, db.PutObject(ctx, &models.Object{
		ID:    seedID,
		Class: "BlastRadiusClassB",
		Properties: map[string]interface{}{
			"stringProp": "seed",
		},
	}, []float32{1, 2, 3, 4}, nil, nil, nil, 0))

	// holdDuration must be long enough that the test reliably observes the
	// blocked-then-unblocked transition without being so long that the test
	// becomes a CI burden. 1.5s is comfortable above scheduling jitter.
	const holdDuration = 1500 * time.Millisecond

	type opCase struct {
		name string
		// category groups ops in the result table — write/read/schema.
		category string
		fn       func() error
	}

	cases := []opCase{
		{
			name:     "PutObject",
			category: "data-write",
			fn: func() error {
				return db.PutObject(ctx, &models.Object{
					ID:    strfmt.UUID(uuid.New().String()),
					Class: "BlastRadiusClassB",
					Properties: map[string]interface{}{
						"stringProp": "blocked-write",
					},
				}, []float32{5, 6, 7, 8}, nil, nil, nil, 0)
			},
		},
		{
			name:     "ObjectByID",
			category: "data-read",
			fn: func() error {
				_, err := db.ObjectByID(ctx, seedID, nil, additional.Properties{}, "")
				return err
			},
		},
		{
			name:     "Exists",
			category: "data-read",
			fn: func() error {
				_, err := db.Exists(ctx, "BlastRadiusClassB", seedID, nil, "")
				return err
			},
		},
		{
			name:     "ObjectSearch",
			category: "data-read",
			fn: func() error {
				_, err := db.ObjectSearch(ctx, 0, 10, nil, nil, additional.Properties{}, "")
				return err
			},
		},
		{
			name:     "AddClass",
			category: "schema-write",
			fn: func() error {
				return migrator.AddClass(ctx, makeTestClass(
					fmt.Sprintf("BlastRadiusOther_%d", time.Now().UnixNano())))
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.category+"/"+tc.name, func(t *testing.T) {
			// Step 1: grab db.indexLock.Lock() in a sidecar goroutine and hold
			// it for `holdDuration`. Signal ready and wait for the test to
			// release.
			ready := make(chan struct{})
			release := make(chan struct{})
			holderDone := make(chan struct{})
			go func() {
				defer close(holderDone)
				db.indexLock.Lock()
				close(ready)
				<-release
				db.indexLock.Unlock()
			}()
			<-ready

			// Step 2: fire the op under test in its own goroutine; it should
			// block on db.indexLock.RLock() (or .Lock() for schema-write).
			start := time.Now()
			opDone := make(chan error, 1)
			go func() {
				opDone <- tc.fn()
			}()

			// Step 3: wait `holdDuration`. The op must NOT have completed by
			// then. If it did, that op does not actually pass through
			// db.indexLock — the static-call-site claim about this op is
			// wrong and we'd want to know.
			select {
			case err := <-opDone:
				close(release)
				<-holderDone
				t.Fatalf("op completed BEFORE lock release "+
					"(elapsed=%v err=%v) — implies %q does NOT grab "+
					"db.indexLock; static-call-site claim wrong",
					time.Since(start), err, tc.name)
			case <-time.After(holdDuration):
			}

			// Step 4: release the lock. The op should complete shortly after.
			close(release)
			<-holderDone

			select {
			case err := <-opDone:
				elapsed := time.Since(start)
				t.Logf("%s blocked for at least %v (full elapsed %v) — confirmed gated by db.indexLock; err=%v",
					tc.name, holdDuration, elapsed, err)
				require.NoError(t, err)
				require.GreaterOrEqual(t, elapsed, holdDuration,
					"op should have blocked at least the lock-hold duration")
			case <-time.After(5 * time.Second):
				t.Fatalf("op %q did not complete within 5s after lock release", tc.name)
			}
		})
	}
}
