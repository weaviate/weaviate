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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/repos/db"
	rCluster "github.com/weaviate/weaviate/cluster"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/config"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// boolPtr is defined in handlers_indexes_gaps_test.go (same package).

// TestNormalizeIndexTypeParam pins the `rangeable` → `rangeFilters` alias
// mapping and out-of-enum rejection.
func TestNormalizeIndexTypeParam(t *testing.T) {
	cases := []struct {
		in      string
		wantTok string
		wantOK  bool
	}{
		{"filterable", "filterable", true},
		{"searchable", "searchable", true},
		{"rangeFilters", "rangeable", true},
		{"rangeable", "rangeable", true},
		{"bogus", "", false},
		{"", "", false},
		{"RangeFilters", "", false}, // case-sensitive; swagger enum is the gate
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			tok, ok := normalizeIndexTypeParam(tc.in)
			assert.Equal(t, tc.wantOK, ok)
			assert.Equal(t, tc.wantTok, tok)
		})
	}
}

// TestCanonicalIndexType pins that "rangeable" always renders as
// "rangeFilters" in responses.
func TestCanonicalIndexType(t *testing.T) {
	assert.Equal(t, "rangeFilters", canonicalIndexType("rangeable"))
	assert.Equal(t, "filterable", canonicalIndexType("filterable"))
	assert.Equal(t, "searchable", canonicalIndexType("searchable"))
}

// textProp / numProp / build a class for the resolver tests.
func textProp(name, tok string, searchable, filterable *bool) *models.Property {
	return &models.Property{
		Name: name, DataType: []string{"text"}, Tokenization: tok,
		IndexSearchable: searchable, IndexFilterable: filterable,
	}
}

// blockmaxStamped sets the durable per-property blockmax stamp, which outranks
// the class-wide flag in db.SearchablePropertyIsBlockmax.
func blockmaxStamped(prop *models.Property, blockmax bool) *models.Property {
	prop.SearchableBlockmax = &blockmax
	return prop
}

func classWith(blockmax bool, props ...*models.Property) *models.Class {
	return &models.Class{
		Properties:          props,
		InvertedIndexConfig: &models.InvertedIndexConfig{UsingBlockMaxWAND: blockmax},
	}
}

// activeReindexTask builds an in-flight reindex task for idempotency/conflict tests.
func activeReindexTask(id, collection string, mt db.ReindexMigrationType, targetTok string, status distributedtask.TaskStatus, props ...string) *distributedtask.Task {
	payload, _ := json.Marshal(db.ReindexTaskPayload{
		Collection: collection, MigrationType: mt, TargetTokenization: targetTok, Properties: props,
	})
	return &distributedtask.Task{
		TaskDescriptor: distributedtask.TaskDescriptor{ID: id},
		Status:         status,
		Payload:        payload,
	}
}

func partlyMigratedRun(t *testing.T, prop string, mt db.ReindexMigrationType, tok string, groups ...string) *distributedtask.Task {
	units := map[string]*distributedtask.Unit{}
	for _, g := range groups {
		units["u"+g] = &distributedtask.Unit{ID: "u" + g, GroupID: g}
	}
	task := buildTask(t, "T", distributedtask.TaskStatusCancelled, db.ReindexTaskPayload{
		Collection: "C", Properties: []string{prop}, MigrationType: mt, TargetTokenization: tok,
	}, units)
	task.SwapDecidedGroups = map[string]time.Time{groups[0]: {}}
	return task
}

// TestResolveSearchableUpsert_Option2 pins that an in-flight task owns the
// outcome: a matching request joins it, a differing one 409s — except when the
// requested configuration is already in place, which joins nothing.
func TestResolveSearchableUpsert_Option2(t *testing.T) {
	on, off := boolPtr(true), boolPtr(false)
	cases := []struct {
		name         string
		prop         *models.Property
		classBM      bool
		tok          string
		algorithm    string
		tasks        []*distributedtask.Task
		wantNoop     bool
		wantJoin     string
		wantConflict bool
		wantMT       db.ReindexMigrationType
	}{
		{
			name:      "repeat blockmax while change-algorithm in flight → joins T1",
			prop:      textProp("t", "word", on, off),
			algorithm: "blockmax",
			tasks:     []*distributedtask.Task{activeReindexTask("T1", "C", db.ReindexTypeChangeAlgorithm, "", distributedtask.TaskStatusStarted, "t")},
			wantNoop:  true,
			wantJoin:  "T1",
		},
		{
			name:     "repeat tokenization while change-tokenization in flight (same target) → joins T1",
			prop:     textProp("t", "word", on, on),
			tok:      "field",
			tasks:    []*distributedtask.Task{activeReindexTask("T1", "C", db.ReindexTypeChangeTokenization, "field", distributedtask.TaskStatusStarted, "t")},
			wantNoop: true,
			wantJoin: "T1",
		},
		{
			name:         "assert current tokenization while migration moves away → 409",
			prop:         textProp("t", "word", on, on),
			tok:          "word",
			tasks:        []*distributedtask.Task{activeReindexTask("T1", "C", db.ReindexTypeChangeTokenization, "field", distributedtask.TaskStatusStarted, "t")},
			wantConflict: true,
		},
		{
			name:         "algorithm request while tokenization migration in flight → 409",
			prop:         textProp("t", "word", on, on),
			algorithm:    "blockmax",
			tasks:        []*distributedtask.Task{activeReindexTask("T1", "C", db.ReindexTypeChangeTokenization, "field", distributedtask.TaskStatusStarted, "t")},
			wantConflict: true,
		},
		{
			name:         "tokenization request while algorithm migration in flight → 409",
			prop:         textProp("t", "word", on, on),
			tok:          "field",
			tasks:        []*distributedtask.Task{activeReindexTask("T1", "C", db.ReindexTypeChangeAlgorithm, "", distributedtask.TaskStatusStarted, "t")},
			wantConflict: true,
		},
		{
			name:     "repeat enable-searchable tokenization while creating (same tok) → joins T1",
			prop:     textProp("t", "word", off, off),
			tok:      "word",
			tasks:    []*distributedtask.Task{activeReindexTask("T1", "C", db.ReindexTypeEnableSearchable, "word", distributedtask.TaskStatusStarted, "t")},
			wantNoop: true,
			wantJoin: "T1",
		},
		{
			name:      "no active task on this property → proceeds to change-algorithm",
			prop:      textProp("t", "word", on, off),
			algorithm: "blockmax",
			tasks:     []*distributedtask.Task{activeReindexTask("T1", "C", db.ReindexTypeChangeAlgorithm, "", distributedtask.TaskStatusStarted, "other")},
			wantMT:    db.ReindexTypeChangeAlgorithm,
		},
		// Regression: an "ensure blockmax" on an already-blockmax property must
		// not be handed an unrelated rebuild's task handle — its caller would
		// then read that rebuild's FAIL/CANCEL as its own failure.
		{
			name:      "already blockmax (class flag) + rebuild-searchable in flight → plain NO_OP",
			prop:      textProp("t", "word", on, off),
			classBM:   true,
			algorithm: "blockmax",
			tasks:     []*distributedtask.Task{activeReindexTask("C:rebuild-searchable:t:ab3f", "C", db.ReindexTypeRebuildSearchable, "", distributedtask.TaskStatusStarted, "t")},
			wantNoop:  true,
		},
		{
			name:      "already blockmax (per-property stamp) + rebuild-searchable in flight → plain NO_OP",
			prop:      blockmaxStamped(textProp("t", "word", on, off), true),
			algorithm: "blockmax",
			tasks:     []*distributedtask.Task{activeReindexTask("C:rebuild-searchable:t:ab3f", "C", db.ReindexTypeRebuildSearchable, "", distributedtask.TaskStatusStarted, "t")},
			wantNoop:  true,
		},
		{
			name:      "class flag set by a class update while a change-algorithm run stands partial → its resubmit runs",
			prop:      textProp("t", "word", on, on),
			classBM:   true,
			algorithm: "blockmax",
			tasks:     []*distributedtask.Task{partlyMigratedRun(t, "t", db.ReindexTypeChangeAlgorithm, "", "a", "b")},
			wantMT:    db.ReindexTypeChangeAlgorithm,
		},
		{
			name:      "already blockmax + change-algorithm in flight → plain NO_OP",
			prop:      textProp("t", "word", on, off),
			classBM:   true,
			algorithm: "blockmax",
			tasks:     []*distributedtask.Task{activeReindexTask("T1", "C", db.ReindexTypeChangeAlgorithm, "", distributedtask.TaskStatusStarted, "t")},
			wantNoop:  true,
		},
		{
			name:      "not yet blockmax + rebuild-searchable in flight → joins T1",
			prop:      textProp("t", "word", on, off),
			algorithm: "blockmax",
			tasks:     []*distributedtask.Task{activeReindexTask("T1", "C", db.ReindexTypeRebuildSearchable, "", distributedtask.TaskStatusStarted, "t")},
			wantNoop:  true,
			wantJoin:  "T1",
		},
		{
			name:         "already blockmax + tokenization migration in flight + algorithm → still 409",
			prop:         textProp("t", "word", on, on),
			classBM:      true,
			algorithm:    "blockmax",
			tasks:        []*distributedtask.Task{activeReindexTask("T1", "C", db.ReindexTypeChangeTokenization, "field", distributedtask.TaskStatusStarted, "t")},
			wantConflict: true,
		},
		// The retokenize path is untouched by the in-place check above: being
		// on blockmax says nothing about the tokenization, so it still joins.
		{
			name:     "already blockmax + retokenize to the in-flight target → joins T1",
			prop:     textProp("t", "word", on, on),
			classBM:  true,
			tok:      "field",
			tasks:    []*distributedtask.Task{activeReindexTask("T1", "C", db.ReindexTypeChangeTokenization, "field", distributedtask.TaskStatusStarted, "t")},
			wantNoop: true,
			wantJoin: "T1",
		},
		{
			name:     "already blockmax + repeat enable-searchable tokenization → joins T1",
			prop:     textProp("t", "word", off, off),
			classBM:  true,
			tok:      "word",
			tasks:    []*distributedtask.Task{activeReindexTask("T1", "C", db.ReindexTypeEnableSearchable, "word", distributedtask.TaskStatusStarted, "t")},
			wantNoop: true,
			wantJoin: "T1",
		},
		{
			name:         "already blockmax + retokenize away from the in-flight target → still 409",
			prop:         textProp("t", "word", on, on),
			classBM:      true,
			tok:          "lowercase",
			tasks:        []*distributedtask.Task{activeReindexTask("T1", "C", db.ReindexTypeChangeTokenization, "field", distributedtask.TaskStatusStarted, "t")},
			wantConflict: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			class := classWith(tc.classBM, tc.prop)
			plan, err := resolveSearchableUpsert(class, "C", tc.prop, tc.tok, tc.algorithm, tc.tasks)
			require.NoError(t, err)
			assert.Equal(t, tc.wantNoop, plan.noop, "noop")
			assert.Equal(t, tc.wantJoin, plan.joinTaskID, "joinTaskID")
			assert.Equal(t, tc.wantConflict, plan.conflict != "", "conflict (got %q)", plan.conflict)
			assert.Equal(t, tc.wantMT, plan.migrationType, "migrationType")
		})
	}
}

// TestResolveSearchableUpsert_InvalidAlgorithmErrorsEvenInFlight pins that an
// invalid/deprecated algorithm 400s even mid-migration — validated before the
// active-task guard, which otherwise matches any non-empty algorithm as NO_OP.
func TestResolveSearchableUpsert_InvalidAlgorithmErrorsEvenInFlight(t *testing.T) {
	on, off := boolPtr(true), boolPtr(false)
	prop := textProp("t", "word", on, off)
	// A change-algorithm task on the SAME property is in flight.
	inFlight := []*distributedtask.Task{
		activeReindexTask("T1", "C", db.ReindexTypeChangeAlgorithm, "", distributedtask.TaskStatusStarted, "t"),
	}
	for _, tc := range []struct {
		name      string
		algorithm string
		wantMsg   string
	}{
		{"wand is deprecated", "wand", "deprecated"},
		{"garbage is unsupported", "sriracha", "unsupported algorithm"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			class := classWith(false, prop)
			plan, err := resolveSearchableUpsert(class, "C", prop, "", tc.algorithm, inFlight)
			require.Error(t, err, "invalid algorithm must 400 even mid-migration, not NO_OP")
			assert.Contains(t, err.Error(), tc.wantMsg)
			assert.False(t, plan.noop, "must not be a NO_OP")
			assert.Empty(t, plan.conflict, "must not be a conflict")
		})
	}
}

// TestResolveUpsertPlan_Searchable covers searchable outcome rows that need
// no DB (create / NO_OP / algorithm / one-change).
func TestResolveUpsertPlan_Searchable(t *testing.T) {
	cases := []struct {
		name     string
		prop     *models.Property
		blockmax bool
		body     *models.IndexUpsertRequest
		wantNoop bool
		wantMT   db.ReindexMigrationType
		wantErr  string
	}{
		{
			name:   "absent + tokenization -> create",
			prop:   textProp("t", "word", boolPtr(false), boolPtr(false)),
			body:   &models.IndexUpsertRequest{Tokenization: "whitespace"},
			wantMT: db.ReindexTypeEnableSearchable,
		},
		{
			name:    "absent + empty body -> 400 tokenization required",
			prop:    textProp("t", "word", boolPtr(false), boolPtr(false)),
			body:    &models.IndexUpsertRequest{},
			wantErr: "tokenization",
		},
		{
			name:     "present + identical tokenization -> NO_OP",
			prop:     textProp("t", "word", boolPtr(true), boolPtr(false)),
			body:     &models.IndexUpsertRequest{Tokenization: "word"},
			wantNoop: true,
		},
		{
			name:     "present + empty body -> NO_OP",
			prop:     textProp("t", "word", boolPtr(true), boolPtr(false)),
			body:     &models.IndexUpsertRequest{},
			wantNoop: true,
		},
		{
			name:   "present + different tokenization -> change-tokenization",
			prop:   textProp("t", "word", boolPtr(true), boolPtr(false)),
			body:   &models.IndexUpsertRequest{Tokenization: "whitespace"},
			wantMT: db.ReindexTypeChangeTokenization,
		},
		{
			name:     "on wand + algorithm blockmax -> change-algorithm",
			prop:     textProp("t", "word", boolPtr(true), boolPtr(false)),
			blockmax: false,
			body:     &models.IndexUpsertRequest{Algorithm: "blockmax"},
			wantMT:   db.ReindexTypeChangeAlgorithm,
		},
		{
			name:     "on wand + algorithm alias bmw -> change-algorithm",
			prop:     textProp("t", "word", boolPtr(true), boolPtr(false)),
			blockmax: false,
			body:     &models.IndexUpsertRequest{Algorithm: "bmw"},
			wantMT:   db.ReindexTypeChangeAlgorithm,
		},
		{
			name:     "already blockmax + algorithm blockmax -> NO_OP",
			prop:     textProp("t", "word", boolPtr(true), boolPtr(false)),
			blockmax: true,
			body:     &models.IndexUpsertRequest{Algorithm: "blockmax"},
			wantNoop: true,
		},
		{
			name:    "algorithm wand -> 400 deprecated",
			prop:    textProp("t", "word", boolPtr(true), boolPtr(false)),
			body:    &models.IndexUpsertRequest{Algorithm: "wand"},
			wantErr: "deprecated",
		},
		{
			name:    "unknown algorithm -> 400",
			prop:    textProp("t", "word", boolPtr(true), boolPtr(false)),
			body:    &models.IndexUpsertRequest{Algorithm: "sparta"},
			wantErr: "unsupported algorithm",
		},
		{
			name:    "tokenization + algorithm both -> 400 one change",
			prop:    textProp("t", "word", boolPtr(true), boolPtr(false)),
			body:    &models.IndexUpsertRequest{Tokenization: "whitespace", Algorithm: "blockmax"},
			wantErr: "at most one",
		},
		{
			name:    "absent searchable + algorithm -> 400 no searchable index",
			prop:    textProp("t", "word", boolPtr(false), boolPtr(false)),
			body:    &models.IndexUpsertRequest{Algorithm: "blockmax"},
			wantErr: "searchable",
		},
		{
			name:    "non-text property -> 400 not a text type",
			prop:    &models.Property{Name: "n", DataType: []string{"int"}, IndexSearchable: boolPtr(false)},
			body:    &models.IndexUpsertRequest{Tokenization: "word"},
			wantErr: "not a text type",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			class := classWith(tc.blockmax, tc.prop)
			plan, err := resolveUpsertPlan(class, "C", tc.prop, "searchable", tc.body, nil, true)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantNoop, plan.noop)
			assert.Equal(t, tc.wantMT, plan.migrationType)
			if tc.wantMT != "" {
				assert.Truef(t, db.IsSemanticMigration(tc.wantMT),
					"upsertIndex 400s a tenant-scoped request for %s on the assumption every type it submits is cluster-wide", tc.wantMT)
			}
		})
	}
}

// TestResolveUpsertPlan_Filterable covers the filterable outcome-matrix rows.
func TestResolveUpsertPlan_Filterable(t *testing.T) {
	cases := []struct {
		name     string
		prop     *models.Property
		body     *models.IndexUpsertRequest
		wantNoop bool
		wantMT   db.ReindexMigrationType
		wantErr  string
	}{
		{
			name:   "absent + empty body -> create with current tokenization",
			prop:   textProp("t", "word", boolPtr(true), boolPtr(false)),
			body:   &models.IndexUpsertRequest{},
			wantMT: db.ReindexTypeEnableFilterable,
		},
		{
			name:   "absent + matching tokenization -> create",
			prop:   textProp("t", "word", boolPtr(true), boolPtr(false)),
			body:   &models.IndexUpsertRequest{Tokenization: "word"},
			wantMT: db.ReindexTypeEnableFilterable,
		},
		{
			name:    "absent + divergent tokenization -> 400",
			prop:    textProp("t", "word", boolPtr(true), boolPtr(false)),
			body:    &models.IndexUpsertRequest{Tokenization: "lowercase"},
			wantErr: "must match",
		},
		{
			name:     "present + empty body -> NO_OP",
			prop:     textProp("t", "word", boolPtr(true), boolPtr(true)),
			body:     &models.IndexUpsertRequest{},
			wantNoop: true,
		},
		{
			name:     "present + identical tokenization -> NO_OP",
			prop:     textProp("t", "word", boolPtr(true), boolPtr(true)),
			body:     &models.IndexUpsertRequest{Tokenization: "word"},
			wantNoop: true,
		},
		{
			name:   "present + different tokenization, no searchable index -> filterable retokenize",
			prop:   textProp("t", "word", boolPtr(false), boolPtr(true)),
			body:   &models.IndexUpsertRequest{Tokenization: "lowercase"},
			wantMT: db.ReindexTypeChangeTokenizationFilterable,
		},
		{
			name:    "present + different tokenization, searchable index too -> 400",
			prop:    textProp("t", "word", boolPtr(true), boolPtr(true)),
			body:    &models.IndexUpsertRequest{Tokenization: "lowercase"},
			wantErr: "would silently desynchronize the searchable index",
		},
		{
			name:    "algorithm on filterable -> 400",
			prop:    textProp("t", "word", boolPtr(true), boolPtr(true)),
			body:    &models.IndexUpsertRequest{Algorithm: "blockmax"},
			wantErr: "only valid for a searchable",
		},
		{
			name:    "blob property -> 400 unsupported",
			prop:    &models.Property{Name: "b", DataType: []string{"blob"}, IndexFilterable: boolPtr(false)},
			body:    &models.IndexUpsertRequest{},
			wantErr: "does not support",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			class := classWith(false, tc.prop)
			plan, err := resolveUpsertPlan(class, "C", tc.prop, "filterable", tc.body, nil, true)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantNoop, plan.noop)
			assert.Equal(t, tc.wantMT, plan.migrationType)
			if tc.wantMT != "" {
				assert.Truef(t, db.IsSemanticMigration(tc.wantMT),
					"upsertIndex 400s a tenant-scoped request for %s on the assumption every type it submits is cluster-wide", tc.wantMT)
			}
		})
	}
}

// TestResolveUpsertPlan_RangeFilters covers rangeFilters (internal token
// "rangeable"): no config fields, create-or-NO_OP, numeric-only.
func TestResolveUpsertPlan_RangeFilters(t *testing.T) {
	numProp := func(rf *bool) *models.Property {
		return &models.Property{Name: "n", DataType: []string{"int"}, IndexRangeFilters: rf}
	}
	cases := []struct {
		name     string
		prop     *models.Property
		body     *models.IndexUpsertRequest
		wantNoop bool
		wantMT   db.ReindexMigrationType
		wantErr  string
	}{
		{
			name:   "absent + empty body -> create",
			prop:   numProp(boolPtr(false)),
			body:   &models.IndexUpsertRequest{},
			wantMT: db.ReindexTypeEnableRangeable,
		},
		{
			name:     "present + empty body -> NO_OP",
			prop:     numProp(boolPtr(true)),
			body:     &models.IndexUpsertRequest{},
			wantNoop: true,
		},
		{
			name:    "tokenization field -> 400 no config",
			prop:    numProp(boolPtr(false)),
			body:    &models.IndexUpsertRequest{Tokenization: "word"},
			wantErr: "no configuration fields",
		},
		{
			name:    "non-numeric property -> 400",
			prop:    textProp("t", "word", boolPtr(true), boolPtr(true)),
			body:    &models.IndexUpsertRequest{},
			wantErr: "numeric",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			class := classWith(false, tc.prop)
			plan, err := resolveUpsertPlan(class, "C", tc.prop, "rangeable", tc.body, nil, true)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantNoop, plan.noop)
			assert.Equal(t, tc.wantMT, plan.migrationType)
			if tc.wantMT != "" {
				assert.Truef(t, db.IsSemanticMigration(tc.wantMT),
					"upsertIndex 400s a tenant-scoped request for %s on the assumption every type it submits is cluster-wide", tc.wantMT)
			}
		})
	}
}

// TestResolveRebuildPlan covers the rebuild preconditions per index type.
func TestResolveRebuildPlan(t *testing.T) {
	cases := []struct {
		name      string
		indexType string
		prop      *models.Property
		blockmax  bool
		tasks     []*distributedtask.Task
		wantMT    db.ReindexMigrationType
		wantErr   string
	}{
		{
			name:      "searchable on blockmax -> rebuild-searchable",
			indexType: "searchable",
			prop:      textProp("t", "word", boolPtr(true), boolPtr(false)),
			blockmax:  true,
			wantMT:    db.ReindexTypeRebuildSearchable,
		},
		{
			name:      "searchable on wand -> 400 blockmax first",
			indexType: "searchable",
			prop:      textProp("t", "word", boolPtr(true), boolPtr(false)),
			blockmax:  false,
			wantErr:   "WAND",
		},
		{
			name:      "searchable absent -> 400",
			indexType: "searchable",
			prop:      textProp("t", "word", boolPtr(false), boolPtr(false)),
			blockmax:  true,
			wantErr:   "searchable",
		},
		{
			name:      "filterable present -> repair-filterable",
			indexType: "filterable",
			prop:      textProp("t", "word", boolPtr(true), boolPtr(true)),
			wantMT:    db.ReindexTypeRepairFilterable,
		},
		{
			name:      "filterable absent -> 400",
			indexType: "filterable",
			prop:      textProp("t", "word", boolPtr(true), boolPtr(false)),
			wantErr:   "does not have a filterable index",
		},
		{
			name:      "rangeFilters present -> repair-rangeable",
			indexType: "rangeable",
			prop:      &models.Property{Name: "n", DataType: []string{"int"}, IndexRangeFilters: boolPtr(true)},
			wantMT:    db.ReindexTypeRepairRangeable,
		},
		{
			name:      "rangeFilters absent -> 400",
			indexType: "rangeable",
			prop:      &models.Property{Name: "n", DataType: []string{"int"}, IndexRangeFilters: boolPtr(false)},
			wantErr:   "does not have a rangeable index",
		},
		{
			name:      "partial filterable run, rebuild searchable -> refused with the resubmit",
			indexType: "searchable",
			prop:      textProp("t", "word", boolPtr(true), boolPtr(true)),
			blockmax:  true,
			tasks:     []*distributedtask.Task{partlyMigratedRun(t, "t", db.ReindexTypeChangeTokenizationFilterable, "field", "a", "b")},
			wantErr:   `1 of 2 tenants of property "t" are migrated; finish the migration with PUT /v1/schema/C/properties/t/index/filterable -d '{"tokenization":"field"}'`,
		},
		{
			name:      "partial single-tenant run -> refused, never named in tenants",
			indexType: "filterable",
			prop:      textProp("t", "word", boolPtr(true), boolPtr(true)),
			tasks:     []*distributedtask.Task{partlyMigratedRun(t, "t", db.ReindexTypeChangeTokenizationFilterable, "field", "")},
			wantErr:   `property "t" of the collection is migrated`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			class := classWith(tc.blockmax, tc.prop)
			class.Class = "C"
			h := &indexesHandlers{}
			plan, err := h.resolveRebuildPlan(class, tc.prop, tc.indexType, tc.tasks, true)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.False(t, plan.noop)
			assert.Equal(t, tc.wantMT, plan.migrationType)
		})
	}
}

// TestValidateTenantScope: NO_OP must reject a mis-scoped tenants param
// just like the submit path, not silently return 200.
func TestValidateTenantScope(t *testing.T) {
	// appState is unused; these cases never reach the DB tenant-existence check.
	h := &indexesHandlers{}
	ctx := context.Background()

	t.Run("empty scope passes on single-tenant collection", func(t *testing.T) {
		require.Nil(t, h.validateTenantScope(ctx, nil, "C", false, false, nil))
	})
	t.Run("empty scope passes on MT semantic op", func(t *testing.T) {
		require.Nil(t, h.validateTenantScope(ctx, nil, "C", true, true, nil))
	})
	t.Run("tenants on single-tenant collection is 400", func(t *testing.T) {
		resp := h.validateTenantScope(ctx, nil, "C", false, false, []string{"t1"})
		code, body := statusOf(t, resp)
		require.Equal(t, http.StatusBadRequest, code)
		require.Len(t, body.Error, 1)
		assert.Contains(t, body.Error[0].Message, "only valid for multi-tenant collections")
	})
	t.Run("tenants on semantic operation is 400", func(t *testing.T) {
		resp := h.validateTenantScope(ctx, nil, "C", true, true, []string{"t1"})
		code, body := statusOf(t, resp)
		require.Equal(t, http.StatusBadRequest, code)
		require.Len(t, body.Error, 1)
		assert.Contains(t, body.Error[0].Message, "cannot be used with semantic migrations")
	})
}

func dbWithTenants(t *testing.T, statuses map[string]string) *db.DB {
	state := &sharding.State{Physical: map[string]sharding.Physical{}}
	for name, status := range statuses {
		state.Physical[name] = sharding.Physical{BelongsToNodes: []string{"node1"}, Status: status}
	}
	reader := schemaUC.NewMockSchemaReader(t)
	reader.EXPECT().Read("C", true, mock.Anything).RunAndReturn(
		func(_ string, _ bool, fn func(*models.Class, *sharding.State) error) error {
			return fn(nil, state)
		})
	database := &db.DB{}
	database.SetSchemaReader(reader)
	return database
}

func TestValidateTenants(t *testing.T) {
	database := dbWithTenants(t, map[string]string{
		"hot":  models.TenantActivityStatusHOT,
		"cold": models.TenantActivityStatusCOLD,
	})

	cases := []struct {
		tenant  string
		wantErr string
	}{
		{tenant: "hot"},
		{tenant: "cold"},
		{tenant: "missing", wantErr: `tenant "missing" does not exist`},
	}
	for _, tc := range cases {
		t.Run(tc.tenant, func(t *testing.T) {
			err := validateTenants(database, context.Background(), "C", []string{tc.tenant})
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Equal(t, tc.wantErr, err.Error())
		})
	}
}

// TestSubmitReindexTask_RefusesNonHotTenants pins that a submit targeting a
// non-HOT tenant starts no task.
func TestSubmitReindexTask_RefusesNonHotTenants(t *testing.T) {
	cases := []struct {
		name     string
		statuses map[string]string
		tenants  []string
		plan     upsertPlan
		wantCode int
		wantErr  string
	}{
		{
			name:     "named COLD tenant",
			statuses: map[string]string{"hot": models.TenantActivityStatusHOT, "cold": models.TenantActivityStatusCOLD},
			tenants:  []string{"cold"},
			plan:     upsertPlan{migrationType: db.ReindexTypeRepairFilterable},
			wantErr:  db.ErrTenantsNotHot.Error(),
		},
		{
			name:     "collection-wide with a COLD tenant",
			statuses: map[string]string{"hot": models.TenantActivityStatusHOT, "cold": models.TenantActivityStatusCOLD},
			plan:     upsertPlan{migrationType: db.ReindexTypeEnableFilterable},
			wantErr:  db.ErrTenantsNotHot.Error(),
		},
		{
			name:     "collection-wide with a FROZEN tenant",
			statuses: map[string]string{"hot": models.TenantActivityStatusHOT, "frozen": models.TenantActivityStatusFROZEN},
			plan:     upsertPlan{migrationType: db.ReindexTypeEnableFilterable},
			wantErr:  db.ErrTenantsNotHot.Error(),
		},
		{
			name:     "collection-wide format-only with an OFFLOADED tenant",
			statuses: map[string]string{"hot": models.TenantActivityStatusHOT, "off": models.TenantActivityStatusOFFLOADED},
			plan:     upsertPlan{migrationType: db.ReindexTypeRepairFilterable},
			wantErr:  db.ErrTenantsNotHot.Error(),
		},
		{
			// Past the tenant check, the stale-state sweep stops the submit
			// because the collection has no local index here.
			name:     "collection-wide with all tenants HOT, ACTIVE or unset",
			statuses: map[string]string{"a": models.TenantActivityStatusHOT, "b": models.TenantActivityStatusACTIVE, "c": ""},
			plan:     upsertPlan{migrationType: db.ReindexTypeEnableFilterable},
			wantCode: http.StatusInternalServerError,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := &indexesHandlers{appState: &state.State{
				Logger: logrus.New(), DB: dbWithTenants(t, tc.statuses), ClusterService: &rCluster.Service{},
			}}
			class := &models.Class{
				Class:              "C",
				MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
				Properties:         []*models.Property{{Name: "p", DataType: []string{"text"}}},
			}

			resp := h.submitReindexTask(context.Background(), nil, class, "C", "p", tc.plan, tc.tenants, nil)

			code, body := statusOf(t, resp)
			if tc.wantErr == "" {
				require.Equal(t, tc.wantCode, code)
				return
			}
			require.Equal(t, http.StatusBadRequest, code)
			require.Len(t, body.Error, 1)
			assert.Equal(t, tc.wantErr, body.Error[0].Message)
		})
	}
}

// Pins that the resubmit skip and the partial status agree on which tenants already serve the target.
func TestMigratedTenantsAcrossRuns(t *testing.T) {
	ownership := map[string][]string{"n1": {"a", "b", "c"}, "n2": {"a", "b", "c"}}
	plan := upsertPlan{
		migrationType: db.ReindexTypeChangeTokenization, targetTok: models.PropertyTokenizationField,
		bucketStrategy: "mapcollection",
	}
	start := time.Now()
	run := func(version uint64, status distributedtask.TaskStatus, mt db.ReindexMigrationType, tok string, held string, decided ...string) *distributedtask.Task {
		task := buildTask(t, "T", status, db.ReindexTaskPayload{
			Collection: "C", Properties: []string{"title"}, MigrationType: mt,
			TargetTokenization: tok, BucketStrategy: "mapcollection",
		}, nil)
		task.Version, task.StartedAt = version, start.Add(time.Duration(version)*time.Minute)
		units := map[string]*distributedtask.Unit{}
		for _, g := range held {
			units[string(g)] = &distributedtask.Unit{ID: string(g), GroupID: string(g)}
		}
		task.Units = units
		task.IndexGroups()
		for _, g := range decided {
			if task.SwapDecidedGroups == nil {
				task.SwapDecidedGroups = map[string]time.Time{}
			}
			task.SwapDecidedGroups[g] = time.Time{}
		}
		return task
	}
	changeTok := db.ReindexTypeChangeTokenization
	field, whitespace := models.PropertyTokenizationField, models.PropertyTokenizationWhitespace
	failed, cancelled := distributedtask.TaskStatusFailed, distributedtask.TaskStatusCancelled
	only := func(tenants ...string) map[string][]string { return map[string][]string{"n1": tenants, "n2": tenants} }

	for _, tc := range []struct {
		name            string
		tasks           []*distributedtask.Task
		want            map[string][]string
		status          string
		migrated, total int64
	}{
		{
			name: "a tenant a replica still owes is rerun, and still reads partial",
			tasks: []*distributedtask.Task{func() *distributedtask.Task {
				task := run(1, failed, changeTok, field, "abc", "a")
				task.SwapsOwed = map[string][]string{"a": {"n2"}}
				return task
			}()},
			want:   ownership,
			status: "partial", migrated: 1, total: 3,
		},
		{
			name:   "every tenant decided: rerun them all, the flip still has to land",
			tasks:  []*distributedtask.Task{run(1, failed, changeTok, field, "abc", "a", "b", "c")},
			want:   ownership,
			status: "partial", migrated: 3, total: 3,
		},
		{
			name: "a resubmit that swapped some adds them to the first run's",
			tasks: []*distributedtask.Task{
				run(1, cancelled, changeTok, field, "abc", "a"),
				run(2, failed, changeTok, field, "bc", "b"),
			},
			want:   only("c"),
			status: "partial", migrated: 2, total: 3,
		},
		{
			name: "a newer swap to another target takes the tenant back",
			tasks: []*distributedtask.Task{
				run(1, failed, changeTok, field, "abc", "a", "b"),
				run(2, failed, changeTok, whitespace, "abc", "b"),
			},
			want:   only("b", "c"),
			status: "partial", migrated: 1, total: 3,
		},
		{
			name: "a collection without tenants reads partial but counts no tenants",
			tasks: []*distributedtask.Task{func() *distributedtask.Task {
				task := run(1, failed, changeTok, field, "", "")
				task.Units = map[string]*distributedtask.Unit{"u": {ID: "u"}}
				task.IndexGroups()
				return task
			}()},
			want:   ownership,
			status: "partial", migrated: 0, total: 0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, skipTenantsAlreadyMigrated(ownership, "C", "title", plan, tc.tasks))

			idx := &models.IndexStatus{Type: "filterable", Status: "ready"}
			mergeReindexStatus(idx, "C", "title", "filterable", db.ReindexRuns(tc.tasks), nil)
			require.Equal(t, tc.status, idx.Status)
			require.Equal(t, tc.migrated, idx.MigratedTenants, "migrated tenants")
			require.Equal(t, tc.total, idx.TotalTenants, "total tenants")
		})
	}
}

// Pins that while a partial run stands, a PUT is refused unless it is the resubmit.
// Searchable and filterable share the property's tokenization, so each blocks the other.
func TestUpsertRefusedOnPartlyMigratedIndex(t *testing.T) {
	word := models.PropertyTokenizationWord
	title := &models.Property{
		Name: "title", DataType: []string{"text"}, Tokenization: word,
		IndexFilterable: boolPtr(true), IndexSearchable: boolPtr(true),
	}
	summary := &models.Property{
		Name: "summary", DataType: []string{"text"}, Tokenization: word,
		IndexFilterable: boolPtr(true), IndexSearchable: boolPtr(false),
	}
	props := map[string]*models.Property{"title": title, "summary": summary}
	class := &models.Class{Class: "C", Properties: []*models.Property{title, summary}, InvertedIndexConfig: &models.InvertedIndexConfig{UsingBlockMaxWAND: true}}
	partialRun := func(id string, status distributedtask.TaskStatus, prop *models.Property, indexType string, req *models.IndexUpsertRequest) *distributedtask.Task {
		plan, err := resolveUpsertPlan(class, "C", prop, indexType, req, nil, true)
		require.NoError(t, err)
		task := buildTask(t, id, status, db.ReindexTaskPayload{
			Collection: "C", Properties: []string{prop.Name}, MigrationType: plan.migrationType,
			TargetTokenization: plan.targetTok, BucketStrategy: plan.bucketStrategy,
		}, map[string]*distributedtask.Unit{"a": {ID: "a", GroupID: "a"}, "b": {ID: "b", GroupID: "b"}})
		task.SwapDecidedGroups = map[string]time.Time{"a": {}}
		return task
	}
	resubmit := &models.IndexUpsertRequest{Tokenization: models.PropertyTokenizationField}
	partials := []*distributedtask.Task{
		partialRun("T", distributedtask.TaskStatusCancelled, title, "searchable", resubmit),
		partialRun("S", distributedtask.TaskStatusCancelled, summary, "filterable", resubmit),
	}
	running := partialRun("R", distributedtask.TaskStatusStarted, title, "searchable", resubmit)
	running.SwapDecidedGroups = nil
	running.Version = partials[0].Version + 1
	const titleResubmit = `PUT /v1/schema/C/properties/title/index/searchable -d '{"tokenization":"field"}'`

	for _, tc := range []struct {
		name              string
		property          string
		indexType         string
		body              *models.IndexUpsertRequest
		resubmitIsRunning bool
		refusal           string // the resubmit the refusal names; empty: not refused
	}{
		{name: "back to the schema's value", property: "title", indexType: "searchable", body: &models.IndexUpsertRequest{Tokenization: word}, refusal: titleResubmit},
		{name: "the resubmit", property: "title", indexType: "searchable", body: resubmit},
		{name: "a repeat of the resubmit while it runs joins it", property: "title", indexType: "searchable", body: resubmit, resubmitIsRunning: true},
		{name: "a request naming no tokenization", property: "title", indexType: "searchable", body: &models.IndexUpsertRequest{}},
		{name: "a partial filterable retokenize, then enabling searchable at the property's tokenization", property: "summary", indexType: "searchable", body: &models.IndexUpsertRequest{Tokenization: word}, refusal: `PUT /v1/schema/C/properties/summary/index/filterable -d '{"tokenization":"field"}'`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tasks := partials
			if tc.resubmitIsRunning {
				tasks = append([]*distributedtask.Task{running}, partials...)
			}
			plan, err := resolveUpsertPlan(class, "C", props[tc.property], tc.indexType, tc.body, tasks, true)
			require.NoError(t, err)
			if tc.resubmitIsRunning {
				require.Equal(t, "R", plan.joinTaskID)
			}
			if tc.refusal == "" {
				require.Empty(t, plan.conflict)
				return
			}
			require.Contains(t, plan.conflict, "1 of 2 tenants")
			require.Contains(t, plan.conflict, tc.refusal)
		})
	}
}

// A run whose cutover landed despite a proposer error, swap still owed: drop and re-add must lock nothing and rebuild every tenant.
func TestDropAndReAddAfterALandedRun(t *testing.T) {
	cases := []struct {
		name   string
		mt     db.ReindexMigrationType
		tok    string
		idx    string
		landed func() *models.Property
		drop   func(*models.Property)
		readd  *models.IndexUpsertRequest
	}{
		{
			"enable-filterable", db.ReindexTypeEnableFilterable, "", "filterable",
			func() *models.Property {
				return &models.Property{Name: "p", DataType: []string{"text"}, Tokenization: "word", IndexFilterable: boolPtr(true), IndexSearchable: boolPtr(true)}
			},
			func(p *models.Property) { p.IndexFilterable = boolPtr(false) }, &models.IndexUpsertRequest{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := tc.landed()
			class := &models.Class{Class: "C", Properties: []*models.Property{p}, InvertedIndexConfig: &models.InvertedIndexConfig{}}

			run := buildTask(t, "T1", distributedtask.TaskStatusSwapping,
				db.ReindexTaskPayload{Collection: "C", Properties: []string{"p"}, MigrationType: tc.mt, TargetTokenization: tc.tok},
				map[string]*distributedtask.Unit{"a": {ID: "a", GroupID: "a"}, "b": {ID: "b", GroupID: "b"}})
			run.SwapDecidedGroups = map[string]time.Time{"a": {}, "b": {}}
			run.SwapsOwed = map[string][]string{"b": {"n2"}}
			require.True(t, db.ReindexDetector{}.CutoverLanded(run, func(string) *models.Class { return class }))
			run.Status = distributedtask.TaskStatusFinished
			tasks := []*distributedtask.Task{run}

			require.NoError(t, db.ReindexDetector{}.CheckPropertyUpdate("C", "p", tasks), "the drop")
			tc.drop(p)
			require.NoError(t, db.ReindexDetector{}.CheckPropertyUpdate("C", "p", tasks), "a change after the drop")
			_, err := (&indexesHandlers{}).resolveRebuildPlan(class, p, "filterable", tasks, true)
			require.NotErrorIs(t, err, errRebuildPropertyPartlyMigrated)
			plan, err := resolveUpsertPlan(class, "C", p, tc.idx, tc.readd, tasks, true)
			require.NoError(t, err)
			require.Empty(t, plan.conflict)
			require.Equal(t, tc.mt, plan.migrationType)
			got := skipTenantsAlreadyMigrated(map[string][]string{"n1": {"a", "b"}, "n2": {"a", "b"}}, "C", "p", plan, tasks)
			require.ElementsMatch(t, []string{"a", "b"}, got["n1"])
		})
	}
}

// Pins that only a caller who may read the tenants is told how many migrated.
func TestPartialRunRefusalsCountTenantsOnlyForTenantReaders(t *testing.T) {
	partial := partlyMigratedRun(t, "title", db.ReindexTypeChangeTokenization, models.PropertyTokenizationField, "a", "b")
	tasks := map[string][]*distributedtask.Task{db.ReindexNamespace: {partial}}
	prop := &models.Property{Name: "title", DataType: []string{"text"}, Tokenization: models.PropertyTokenizationWord, IndexFilterable: boolPtr(true), IndexSearchable: boolPtr(true)}
	class := &models.Class{Class: "C", Properties: []*models.Property{prop}, InvertedIndexConfig: &models.InvertedIndexConfig{UsingBlockMaxWAND: true}}

	indexes := func(t *testing.T, authz authorization.Authorizer) *indexesHandlers {
		reader := schemaUC.NewMockSchemaReader(t)
		reader.EXPECT().ReadOnlyClass("C").Return(class)
		return &indexesHandlers{appState: &state.State{
			Authorizer: authz, SchemaManager: &schemaUC.Manager{SchemaReader: reader},
			ReindexSubmitLocks: state.NewReindexSubmitLocks(), Logger: logrus.New(),
			ServerConfig: &config.WeaviateConfig{Config: config.Config{RuntimeReindexEnabled: true}},
		}, taskLister: fakeReindexTaskLister{tasks: tasks}}
	}
	for _, tc := range []struct {
		name     string
		wantCode int
		call     func(t *testing.T, authz authorization.Authorizer) middleware.Responder
	}{
		{name: "upsert", wantCode: http.StatusConflict, call: func(t *testing.T, authz authorization.Authorizer) middleware.Responder {
			return indexes(t, authz).upsertIndex(schema.SchemaObjectsIndexUpsertParams{
				HTTPRequest: httptest.NewRequest(http.MethodPut, "/", nil), ClassName: "C", PropertyName: "title",
				IndexName: "searchable", Body: &models.IndexUpsertRequest{Tokenization: models.PropertyTokenizationWhitespace},
			}, nil)
		}},
		{name: "rebuild", wantCode: http.StatusConflict, call: func(t *testing.T, authz authorization.Authorizer) middleware.Responder {
			return indexes(t, authz).rebuildIndex(schema.SchemaObjectsIndexRebuildParams{
				HTTPRequest: httptest.NewRequest(http.MethodPost, "/", nil), ClassName: "C", PropertyName: "title", IndexName: "filterable",
			}, nil)
		}},
		{name: "index delete", wantCode: http.StatusUnprocessableEntity, call: func(t *testing.T, authz authorization.Authorizer) middleware.Responder {
			h := gateWithTasks(partial)
			h.authorizer = authz
			return h.deleteClassPropertyIndex(schema.SchemaObjectsPropertiesDeleteParams{
				HTTPRequest: httptest.NewRequest(http.MethodDelete, "/", nil), ClassName: "C", PropertyName: "title", IndexName: "filterable",
			}, nil)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, reader := range []struct {
				authz      authorization.Authorizer
				seesCounts bool
			}{{&authorization.DummyAuthorizer{}, true}, {&collectionsOnly{}, false}} {
				rec := httptest.NewRecorder()
				tc.call(t, reader.authz).WriteResponse(rec, runtime.JSONProducer())
				require.Equal(t, tc.wantCode, rec.Code, rec.Body.String())
				require.Equal(t, reader.seesCounts, strings.Contains(rec.Body.String(), "1 of 2 tenants"), rec.Body.String())
			}
		})
	}
}
