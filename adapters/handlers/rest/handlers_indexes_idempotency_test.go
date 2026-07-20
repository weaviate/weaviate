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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
)

// TestResolveFilterableUpsert_Idempotency pins declarative idempotency for the
// filterable resolver: a repeat PUT while a converging task is in flight
// NO-OPs (200) instead of resubmitting into a downstream 409, and a divergent
// requested change 409s.
func TestResolveFilterableUpsert_Idempotency(t *testing.T) {
	on, off := boolPtr(true), boolPtr(false)
	cases := []struct {
		name         string
		prop         *models.Property
		body         *models.IndexUpsertRequest
		tasks        []*distributedtask.Task
		wantNoop     bool
		wantConflict bool
		wantMT       db.ReindexMigrationType
	}{
		{
			name:     "empty-body repeat while enable-filterable in flight -> NO_OP",
			prop:     textProp("t", "word", on, off),
			body:     &models.IndexUpsertRequest{},
			tasks:    []*distributedtask.Task{activeReindexTask("T1", "C", db.ReindexTypeEnableFilterable, "", distributedtask.TaskStatusStarted, "t")},
			wantNoop: true,
		},
		{
			name:     "matching-tok repeat while enable-filterable in flight -> NO_OP",
			prop:     textProp("t", "word", on, off),
			body:     &models.IndexUpsertRequest{Tokenization: "word"},
			tasks:    []*distributedtask.Task{activeReindexTask("T1", "C", db.ReindexTypeEnableFilterable, "", distributedtask.TaskStatusStarted, "t")},
			wantNoop: true,
		},
		{
			name:     "same-target retokenize while change-tokenization-filterable in flight -> NO_OP",
			prop:     textProp("t", "word", on, on),
			body:     &models.IndexUpsertRequest{Tokenization: "field"},
			tasks:    []*distributedtask.Task{activeReindexTask("T1", "C", db.ReindexTypeChangeTokenizationFilterable, "field", distributedtask.TaskStatusStarted, "t")},
			wantNoop: true,
		},
		{
			name:     "same-target retokenize while coupled change-tokenization in flight -> NO_OP",
			prop:     textProp("t", "word", on, on),
			body:     &models.IndexUpsertRequest{Tokenization: "field"},
			tasks:    []*distributedtask.Task{activeReindexTask("T1", "C", db.ReindexTypeChangeTokenization, "field", distributedtask.TaskStatusStarted, "t")},
			wantNoop: true,
		},
		{
			name:         "different-target retokenize while retokenize in flight -> 409",
			prop:         textProp("t", "word", on, on),
			body:         &models.IndexUpsertRequest{Tokenization: "lowercase"},
			tasks:        []*distributedtask.Task{activeReindexTask("T1", "C", db.ReindexTypeChangeTokenizationFilterable, "field", distributedtask.TaskStatusStarted, "t")},
			wantConflict: true,
		},
		{
			name:         "assert-current retokenize while retokenize moves away -> 409",
			prop:         textProp("t", "word", on, on),
			body:         &models.IndexUpsertRequest{Tokenization: "word"},
			tasks:        []*distributedtask.Task{activeReindexTask("T1", "C", db.ReindexTypeChangeTokenizationFilterable, "field", distributedtask.TaskStatusStarted, "t")},
			wantConflict: true,
		},
		{
			name:     "empty body while unrelated retokenize in flight on same prop -> NO_OP (index exists)",
			prop:     textProp("t", "word", on, on),
			body:     &models.IndexUpsertRequest{},
			tasks:    []*distributedtask.Task{activeReindexTask("T1", "C", db.ReindexTypeChangeTokenizationFilterable, "field", distributedtask.TaskStatusStarted, "t")},
			wantNoop: true,
		},
		{
			name:   "no active task on this property -> proceeds to enable-filterable",
			prop:   textProp("t", "word", on, off),
			body:   &models.IndexUpsertRequest{},
			tasks:  []*distributedtask.Task{activeReindexTask("T1", "C", db.ReindexTypeEnableFilterable, "", distributedtask.TaskStatusStarted, "other")},
			wantMT: db.ReindexTypeEnableFilterable,
		},
	}
	h := &indexesHandlers{}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			class := classWith(false, tc.prop)
			plan, err := h.resolveUpsertPlan(class, "C", tc.prop, "filterable", tc.body, tc.tasks)
			require.NoError(t, err)
			assert.Equal(t, tc.wantNoop, plan.noop, "noop")
			assert.Equal(t, tc.wantConflict, plan.conflict != "", "conflict (got %q)", plan.conflict)
			assert.Equal(t, tc.wantMT, plan.migrationType, "migrationType")
		})
	}
}

// TestResolveRangeableUpsert_Idempotency pins that a repeat PUT .../rangeFilters
// while an enable-rangeable is in flight NO-OPs (200) instead of a downstream
// 409. rangeFilters takes no config, so there is no divergent case.
func TestResolveRangeableUpsert_Idempotency(t *testing.T) {
	numProp := func(rf *bool) *models.Property {
		return &models.Property{Name: "n", DataType: []string{"int"}, IndexRangeFilters: rf}
	}
	off := boolPtr(false)
	cases := []struct {
		name     string
		prop     *models.Property
		tasks    []*distributedtask.Task
		wantNoop bool
		wantMT   db.ReindexMigrationType
	}{
		{
			name:     "repeat while enable-rangeable in flight -> NO_OP",
			prop:     numProp(off),
			tasks:    []*distributedtask.Task{activeReindexTask("T1", "C", db.ReindexTypeEnableRangeable, "", distributedtask.TaskStatusStarted, "n")},
			wantNoop: true,
		},
		{
			name:     "repeat while repair-rangeable in flight -> NO_OP",
			prop:     numProp(off),
			tasks:    []*distributedtask.Task{activeReindexTask("T1", "C", db.ReindexTypeRepairRangeable, "", distributedtask.TaskStatusStarted, "n")},
			wantNoop: true,
		},
		{
			name:   "no active task on this property -> proceeds to enable-rangeable",
			prop:   numProp(off),
			tasks:  []*distributedtask.Task{activeReindexTask("T1", "C", db.ReindexTypeEnableRangeable, "", distributedtask.TaskStatusStarted, "other")},
			wantMT: db.ReindexTypeEnableRangeable,
		},
	}
	h := &indexesHandlers{}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			class := classWith(false, tc.prop)
			plan, err := h.resolveUpsertPlan(class, "C", tc.prop, "rangeable", &models.IndexUpsertRequest{}, tc.tasks)
			require.NoError(t, err)
			assert.Equal(t, tc.wantNoop, plan.noop, "noop")
			assert.Equal(t, tc.wantMT, plan.migrationType, "migrationType")
		})
	}
}
