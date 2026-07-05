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

// Package reindex_rangeable_concurrent_writes is the black-box regression
// suite for weaviate/weaviate#11688: live writes racing an enable-rangeable
// runtime migration must not be dropped from the rangeable index. A live
// control class (no migration) pins that the write storm itself loses
// nothing.
package reindex_rangeable_concurrent_writes

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

const (
	// numObjects keeps the backfill long enough that the M concurrent
	// PATCHes overlap the migration window, while keeping CI runtime sane.
	numObjects = 4000
	numUpdates = 500
	// updateThreads fans the PATCHes out across goroutines.
	updateThreads = 4
	// mark is the post-update property value; far above every initial
	// value so `vint >= mark` counts exactly the updated objects.
	mark = 777777

	propName = "vint"
)

func TestEnableRangeable_ConcurrentWrites(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviate().
		// 1s scheduler tick keeps the submit→migration-start latency low so
		// the PATCH storm reliably overlaps the migration window.
		WithWeaviateEnv("DISTRIBUTED_TASKS_SCHEDULER_TICK_INTERVAL_SECONDS", "1").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	restURI := compose.GetWeaviate().URI()

	// Dump container logs on failure.
	container := compose.GetWeaviate().Container()
	defer func() {
		if t.Failed() {
			reader, err := container.Logs(ctx)
			if err != nil {
				t.Logf("failed to get container logs: %v", err)
				return
			}
			defer reader.Close()
			logs, _ := io.ReadAll(reader)
			if len(logs) > 16384 {
				logs = logs[len(logs)-16384:]
			}
			t.Logf("=== Container logs (tail) ===\n%s", string(logs))
		}
	}()

	// Live control FIRST: same storm, no migration — isolates the migration
	// as the cause if only the migration case fails.
	t.Run("live control (no migration)", func(t *testing.T) {
		className := "F10RangeableLive"
		ids := setupClassWithObjects(t, className, true)
		runUpdateStorm(t, restURI, className, ids)

		require.Eventually(t, func() bool {
			return aggregateCountWhereGTE(t, className, propName, mark) == numUpdates
		}, 60*time.Second, time.Second,
			"control: all %d updated objects must be served at >= %d", numUpdates, mark)
	})

	t.Run("enable-rangeable migration with concurrent writes", func(t *testing.T) {
		className := "F10RangeableMig"
		ids := setupClassWithObjects(t, className, false)

		taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, className, propName,
			`{"rangeable":{"enabled":true}}`)
		t.Logf("submitted enable-rangeable task: %s", taskID)

		// Fire the PATCH storm immediately so it overlaps the migration
		// window (markStarted → backfill → swap → schema flip).
		runUpdateStorm(t, restURI, className, ids)

		reindexhelpers.AwaitReindexViaIndexes(t, restURI, className, propName, "rangeable")
		reindexhelpers.AwaitReindexFinished(t, restURI, taskID)

		// Schema flag must be flipped.
		updatedClass := helper.GetClass(t, className)
		for _, prop := range updatedClass.Properties {
			if prop.Name == propName {
				require.NotNil(t, prop.IndexRangeFilters)
				require.True(t, *prop.IndexRangeFilters)
			}
		}

		// The core assertion: every concurrently PATCHed object must be
		// range-queryable (weaviate/weaviate#11688).
		var got int
		require.Eventuallyf(t, func() bool {
			got = aggregateCountWhereGTE(t, className, propName, mark)
			return got == numUpdates
		}, 60*time.Second, time.Second,
			"migration must not drop concurrent writes from the rangeable index: "+
				"Aggregate(vint >= %d) = %d, want %d", mark, got, numUpdates)

		// Sanity: total object count unchanged.
		require.Equal(t, numObjects, aggregateCountAll(t, className),
			"total object count must be unchanged by the migration")
	})
}

// setupClassWithObjects creates the test class and imports numObjects objects
// with vint cycling 0..99. rangeableAtCreate toggles the live-control class
// (index enabled at create) vs the migration class (enabled at runtime);
// indexFilterable is false in both, the property state that exposed the bug.
func setupClassWithObjects(t *testing.T, className string, rangeableAtCreate bool) []string {
	t.Helper()
	vFalse := false
	class := &models.Class{
		Class:      className,
		Vectorizer: "none",
		Properties: []*models.Property{
			{
				Name:     "grp",
				DataType: schema.DataTypeInt.PropString(),
			},
			{
				Name:              propName,
				DataType:          schema.DataTypeInt.PropString(),
				IndexFilterable:   &vFalse,
				IndexRangeFilters: &rangeableAtCreate,
			},
		},
	}
	helper.CreateClass(t, class)
	t.Cleanup(func() { helper.DeleteClass(t, className) })

	ids := make([]string, numObjects)
	for i := 0; i < numObjects; i++ {
		ids[i] = uuid.NewSHA1(uuid.NameSpaceDNS, []byte(fmt.Sprintf("%s-%d", className, i))).String()
	}

	const batchSize = 1000
	for from := 0; from < numObjects; from += batchSize {
		to := from + batchSize
		if to > numObjects {
			to = numObjects
		}
		batch := make([]*models.Object, 0, to-from)
		for i := from; i < to; i++ {
			batch = append(batch, &models.Object{
				Class: className,
				ID:    strfmt.UUID(ids[i]),
				Properties: map[string]interface{}{
					"grp":    int64(1),
					propName: int64(i % 100),
				},
			})
		}
		helper.CreateObjectsBatch(t, batch)
	}
	return ids
}

// runUpdateStorm PATCHes numUpdates objects to vint=mark and waits for
// completion. Every PATCH must succeed, else the count assertion is ambiguous.
func runUpdateStorm(t *testing.T, restURI, className string, ids []string) {
	t.Helper()
	logger := logrus.New()

	targets := ids[1000 : 1000+numUpdates]
	errCh := make(chan error, numUpdates)
	var wg sync.WaitGroup
	for th := 0; th < updateThreads; th++ {
		chunk := make([]string, 0, numUpdates/updateThreads+1)
		for i := th; i < len(targets); i += updateThreads {
			chunk = append(chunk, targets[i])
		}
		wg.Add(1)
		enterrors.GoWrapper(func() {
			defer wg.Done()
			for _, id := range chunk {
				if err := patchVint(restURI, className, id); err != nil {
					errCh <- fmt.Errorf("patch %s: %w", id, err)
				}
			}
		}, logger)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Errorf("update storm: %v", err)
	}
	require.False(t, t.Failed(), "all %d PATCHes must succeed", numUpdates)
	t.Logf("update storm complete: %d PATCHes", numUpdates)
}

// patchVint PATCHes a single object's vint to mark via the REST API. Raw
// HTTP keeps error handling goroutine-safe (no testify asserts off the test
// goroutine).
func patchVint(restURI, className, id string) error {
	body, err := json.Marshal(map[string]interface{}{
		"class":      className,
		"id":         id,
		"properties": map[string]interface{}{propName: mark},
	})
	if err != nil {
		return err
	}
	url := fmt.Sprintf("http://%s/v1/objects/%s/%s", restURI, className, id)
	req, err := http.NewRequest(http.MethodPatch, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	// Strictly 204: an empty 200 is the signature of a panicked handler — the
	// REST panic middleware recovers without writing a response, so net/http
	// defaults to 200 with an empty body while the write was half-applied.
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("status %d (want 204): %s", resp.StatusCode, string(respBody))
	}
	return nil
}

// aggregateCountWhereGTE returns Aggregate{meta{count}} for prop >= value
// — served by the rangeable index once the schema flag is on.
func aggregateCountWhereGTE(t *testing.T, className, prop string, value int) int {
	t.Helper()
	query := fmt.Sprintf(`{
		Aggregate {
			%s(where: {path:[%q], operator: GreaterThanEqual, valueInt: %d}) {
				meta { count }
			}
		}
	}`, className, prop, value)
	return aggregateMetaCount(t, className, query)
}

// aggregateCountAll returns the unfiltered Aggregate{meta{count}}.
func aggregateCountAll(t *testing.T, className string) int {
	t.Helper()
	query := fmt.Sprintf(`{
		Aggregate {
			%s {
				meta { count }
			}
		}
	}`, className)
	return aggregateMetaCount(t, className, query)
}

func aggregateMetaCount(t *testing.T, className, query string) int {
	t.Helper()
	resp, err := graphqlhelper.QueryGraphQL(t, nil, "", query, nil)
	require.NoError(t, err)
	require.Empty(t, resp.Errors, "graphql errors: %+v", resp.Errors)

	var data map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(mustMarshal(t, resp.Data), &data))
	var agg map[string][]struct {
		Meta struct {
			Count int `json:"count"`
		} `json:"meta"`
	}
	require.NoError(t, json.Unmarshal(data["Aggregate"], &agg))
	require.NotEmpty(t, agg[className], "Aggregate returned no rows for %s", className)
	return agg[className][0].Meta.Count
}

func mustMarshal(t *testing.T, v interface{}) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}
