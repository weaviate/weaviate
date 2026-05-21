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

package reindex_singlenode

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/helper"
)

// testReindexScopeAssertion pins the "blast radius" of every runtime-reindex
// migration type. For each migration it creates a collection with multiple
// eligible properties, submits a reindex against ONE of them, and then
// asserts that:
//
//  1. The task payload under /v1/tasks reports exactly the targeted property
//     (not an empty list, not a superset).
//  2. A background poller of /v1/schema/{class}/indexes observes non-targeted
//     properties in `ready` state for the entire migration window — they
//     must never flip to `indexing` or `pending`.
//  3. The taskId returned from submit encodes the targeted property.
//
// This is deliberately structural rather than data-correctness: data
// correctness is already covered by the per-migration tests. What this test
// adds is the axis those tests were blind to — whether the migration stayed
// inside the requested scope. A bug that silently widens the rebuild (as
// happened historically with repair-searchable and repair-filterable) would
// flip ALL searchable/filterable properties to indexing and fail assertion 2,
// and would submit a payload with Properties=nil and fail assertion 1.
//
// This test is structurally migration-type-agnostic: adding a fifth
// migration type is one new row in the table below.
func testReindexScopeAssertion(t *testing.T, restURI string) {
	type scopeCase struct {
		name       string
		className  string
		properties []*models.Property
		objects    []map[string]interface{}
		target     string
		body       string
		// indexType is the key under which the targeted property's index
		// entry appears in GET /v1/schema/{class}/indexes. Only that entry
		// is allowed to leave `ready` during the migration; every other
		// (property, indexType) pair must stay `ready`.
		indexType string
	}

	textScopeObjects := make([]map[string]interface{}, 0, 500)
	for i := 0; i < 500; i++ {
		textScopeObjects = append(textScopeObjects, map[string]interface{}{
			"title":       fmt.Sprintf("alpha beta gamma %d", i),
			"description": fmt.Sprintf("lorem ipsum dolor %d sit amet", i),
			"author":      fmt.Sprintf("author_%d", i%17),
			"score":       float64(i),
			"quantity":    int64(i % 97),
			"released":    time.Now().Add(time.Duration(i) * time.Hour).UTC().Format(time.RFC3339),
		})
	}

	trueVal := true
	rangeScopeProps := []*models.Property{
		{Name: "score", DataType: []string{"number"}, IndexFilterable: &trueVal},
		{Name: "quantity", DataType: []string{"int"}, IndexFilterable: &trueVal},
		{Name: "released", DataType: []string{"date"}, IndexFilterable: &trueVal},
	}

	textProps := []*models.Property{
		{Name: "title", DataType: []string{"text"}, Tokenization: "word", IndexSearchable: &trueVal, IndexFilterable: &trueVal},
		{Name: "description", DataType: []string{"text"}, Tokenization: "word", IndexSearchable: &trueVal, IndexFilterable: &trueVal},
		{Name: "author", DataType: []string{"text"}, Tokenization: "field", IndexSearchable: &trueVal, IndexFilterable: &trueVal},
	}

	// Filterable-refresh fixture needs non-text props too so the
	// "all other properties stay ready" assertion covers int/number/date.
	filterableScopeProps := append([]*models.Property{}, textProps...)
	filterableScopeProps = append(filterableScopeProps, rangeScopeProps...)

	// Searchable-rebuild fixture: purely text props with multiple searchables.
	searchableScopeProps := append([]*models.Property{}, textProps...)

	// Change-tokenization fixture: same shape as searchable, but we only target one.
	tokenizationScopeProps := append([]*models.Property{}, textProps...)

	// Enable-rangeable fixture: multiple numeric props, only one gets rangeable enabled.
	enableRangeableProps := append([]*models.Property{}, rangeScopeProps...)

	// Enable-filterable fixture: multiple non-filterable numeric/boolean props,
	// only one gets filterable enabled. Filterable=false is explicit so the
	// pre-migration state is unambiguous.
	falseVal := false
	enableFilterableProps := []*models.Property{
		{Name: "score", DataType: []string{"number"}, IndexFilterable: &falseVal},
		{Name: "quantity", DataType: []string{"int"}, IndexFilterable: &falseVal},
		{Name: "active", DataType: []string{"boolean"}, IndexFilterable: &falseVal},
	}

	// Enable-searchable fixture: multiple non-searchable text props, only one
	// gets searchable enabled. The migration uses tokenization "word"; the
	// filterable indexes on the OTHER props (body, tag) intentionally have a
	// matching tokenization so the divergent-tokenization guard doesn't reject
	// the request. title has no filterable (truly from-scratch) so the scope
	// poller can observe the enable-searchable transition cleanly.
	enableSearchableProps := []*models.Property{
		{Name: "title", DataType: []string{"text"}, IndexSearchable: &falseVal, IndexFilterable: &falseVal},
		{Name: "body", DataType: []string{"text"}, Tokenization: "word", IndexSearchable: &falseVal, IndexFilterable: &trueVal},
		{Name: "tag", DataType: []string{"text"}, Tokenization: "word", IndexSearchable: &falseVal, IndexFilterable: &trueVal},
	}

	// The scope-assertion fixture uses textScopeObjects, which carries keys
	// "title", "description", "author", "score", "quantity", "released".
	// For enable-filterable we need a "score"/"quantity"/"active" object set
	// — synthesize a small one inline.
	enableFilterableObjects := make([]map[string]interface{}, 0, 200)
	for i := 0; i < 200; i++ {
		enableFilterableObjects = append(enableFilterableObjects, map[string]interface{}{
			"score":    float64(i),
			"quantity": int64(i % 97),
			"active":   i%2 == 0,
		})
	}

	// And for enable-searchable: title/body/tag.
	enableSearchableObjects := make([]map[string]interface{}, 0, 200)
	for i := 0; i < 200; i++ {
		enableSearchableObjects = append(enableSearchableObjects, map[string]interface{}{
			"title": fmt.Sprintf("doc_%d", i),
			"body":  fmt.Sprintf("lorem ipsum dolor %d sit amet", i),
			"tag":   fmt.Sprintf("tag_%d", i%17),
		})
	}

	cases := []scopeCase{
		{
			name:       "change-algorithm",
			className:  "ScopeRepairSearchable",
			properties: searchableScopeProps,
			objects:    textScopeObjects,
			target:     "title",
			body:       `{"searchable":{"algorithm":"blockmax"}}`,
			indexType:  "searchable",
		},
		{
			name:       "repair-filterable",
			className:  "ScopeRepairFilterable",
			properties: filterableScopeProps,
			objects:    textScopeObjects,
			target:     "author",
			body:       `{"filterable":{"rebuild":true}}`,
			indexType:  "filterable",
		},
		{
			name:       "change-tokenization",
			className:  "ScopeChangeTokenization",
			properties: tokenizationScopeProps,
			objects:    textScopeObjects,
			target:     "description",
			body:       `{"searchable":{"tokenization":"whitespace"}}`,
			// Tokenization touches both searchable + filterable for the
			// target property. The assertion on non-target props applies
			// to both index types: the non-target props' searchable AND
			// filterable entries must stay ready.
			indexType: "searchable",
		},
		{
			name:       "enable-rangeable",
			className:  "ScopeEnableRangeable",
			properties: enableRangeableProps,
			objects:    textScopeObjects,
			target:     "score",
			body:       `{"rangeable":{"enabled":true}}`,
			indexType:  "rangeable",
		},
		{
			name:       "enable-filterable",
			className:  "ScopeEnableFilterable",
			properties: enableFilterableProps,
			objects:    enableFilterableObjects,
			target:     "score",
			body:       `{"filterable":{"enabled":true}}`,
			indexType:  "filterable",
		},
		{
			name:       "enable-searchable",
			className:  "ScopeEnableSearchable",
			properties: enableSearchableProps,
			objects:    enableSearchableObjects,
			target:     "title",
			body:       `{"searchable":{"enabled":true,"tokenization":"word"}}`,
			indexType:  "searchable",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			class := &models.Class{
				Class:      tc.className,
				Properties: tc.properties,
				Vectorizer: "none",
			}
			helper.CreateClass(t, class)
			defer helper.DeleteClass(t, tc.className)

			for i, props := range tc.objects {
				obj := &models.Object{Class: tc.className, Properties: props}
				require.NoError(t, helper.CreateObject(t, obj), "failed to create object %d", i)
			}

			// Build the set of (property, indexType) pairs that should stay
			// `ready` for the entire migration window.
			//
			// The set is the cross-product of [non-target properties] ×
			// [index types that this migration could legitimately touch].
			// Specifically we must watch the exact index types the migration
			// could affect — change-tokenization, for example, touches BOTH
			// searchable and filterable, so both entries of every non-target
			// property must stay ready.
			watchedIndexTypes := []string{tc.indexType}
			if tc.name == "change-tokenization" {
				watchedIndexTypes = []string{"searchable", "filterable"}
			}

			shouldStayReady := make(map[string]map[string]bool) // propName -> indexType -> true
			for _, p := range tc.properties {
				if p.Name == tc.target {
					continue
				}
				shouldStayReady[p.Name] = make(map[string]bool)
				for _, it := range watchedIndexTypes {
					shouldStayReady[p.Name][it] = true
				}
			}

			// Background poller: samples the indexes endpoint at a high
			// rate and records any non-`ready` status observed on a
			// property/index pair that should have stayed ready.
			type violation struct {
				property  string
				indexType string
				status    string
				progress  float32
				at        time.Time
			}
			var (
				violations   []violation
				violationsMu sync.Mutex
				samples      atomic.Int64
			)

			stopCh := make(chan struct{})
			var wg sync.WaitGroup
			// Defer the poller shutdown so a mid-test t.FailNow (e.g. a 400
			// response from submit) still drains the goroutine — otherwise
			// it would keep calling require.NoError on a completed parent
			// test, which Go's testing package surfaces as a panic.
			stopOnce := sync.Once{}
			stopPoller := func() {
				stopOnce.Do(func() { close(stopCh) })
				wg.Wait()
			}
			defer stopPoller()
			wg.Add(1)
			go func() {
				defer wg.Done()
				ticker := time.NewTicker(50 * time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-stopCh:
						return
					case <-ticker.C:
					}
					resp := reindexhelpers.GetIndexes(t, restURI, tc.className)
					samples.Add(1)
					for _, prop := range resp.Properties {
						allowed, ok := shouldStayReady[prop.Name]
						if !ok {
							continue
						}
						for _, idx := range prop.Indexes {
							if !allowed[idx.Type] {
								continue
							}
							if idx.Status != "ready" {
								violationsMu.Lock()
								violations = append(violations, violation{
									property:  prop.Name,
									indexType: idx.Type,
									status:    idx.Status,
									progress:  idx.Progress,
									at:        time.Now(),
								})
								violationsMu.Unlock()
							}
						}
					}
				}
			}()

			// Submit the reindex and wait for it to finish.
			taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, tc.className, tc.target, tc.body)
			t.Logf("submitted reindex task: %s", taskID)

			// Assertion 3: the task ID must encode the targeted property.
			// Task IDs are formatted as "Collection:migration-type:property:suffix".
			assert.Contains(t, taskID, ":"+tc.target+":",
				"task ID %q does not encode the targeted property %q", taskID, tc.target)

			// Assertion 1: the task payload on /v1/tasks must report exactly
			// the targeted property. Grab it before the task finishes —
			// after FINISHED the task may be pruned, so capture early.
			assertPayloadProperties(t, restURI, taskID, tc.target)

			reindexhelpers.AwaitReindexFinished(t, restURI, taskID)

			stopPoller()

			t.Logf("scope poller: %d samples taken", samples.Load())

			// Assertion 2: no non-target property ever left `ready`.
			violationsMu.Lock()
			defer violationsMu.Unlock()
			if len(violations) > 0 {
				for _, v := range violations {
					t.Errorf("blast radius leak: %s/%s saw status=%s progress=%.2f at %s",
						v.property, v.indexType, v.status, v.progress,
						v.at.Format(time.RFC3339Nano))
				}
				t.Fatalf("%d scope violations observed — migration %q touched properties it shouldn't have",
					len(violations), tc.name)
			}
		})
	}
}

// assertPayloadProperties polls /v1/tasks for the given taskID and asserts
// that its payload contains exactly [target] in the properties array. It
// tolerates the task already being FINISHED as long as the payload is still
// readable, but fails if the task disappears before we can observe it.
func assertPayloadProperties(t *testing.T, restURI, taskID, target string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(fmt.Sprintf("http://%s/v1/tasks", restURI))
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		// Use a map[string]interface{} deserialization for the task since
		// Payload is typed as interface{} and will come back as a generic
		// JSON object.
		var envelope map[string][]struct {
			ID      string                 `json:"id"`
			Payload map[string]interface{} `json:"payload"`
		}
		if err := json.Unmarshal(body, &envelope); err != nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		for _, task := range envelope["reindex"] {
			if task.ID != taskID {
				continue
			}
			rawProps, ok := task.Payload["properties"]
			require.True(t, ok, "task %s payload has no `properties` field (payload=%+v)", taskID, task.Payload)
			propsList, ok := rawProps.([]interface{})
			require.True(t, ok, "task %s payload.properties is not a list: %T", taskID, rawProps)
			require.Len(t, propsList, 1,
				"task %s payload.properties should contain exactly one entry but has %d: %v",
				taskID, len(propsList), propsList)
			require.Equal(t, target, propsList[0],
				"task %s payload.properties should be [%q] but is %v",
				taskID, target, propsList)
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("task %s not found in /v1/tasks within 30s — could not verify payload scope", taskID)
}
