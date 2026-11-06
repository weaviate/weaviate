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
	"encoding/json"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
)

var namedResubmit = regexp.MustCompile(`PUT /v1/schema/C/properties/(\w+)/index/(\w+) -d '([^']*)'`)

type lockCase struct {
	name      string
	indexType string
	req       models.IndexUpsertRequest
	flag      bool
	prop      func() *models.Property
	toTarget  func(*models.Property)
}

func lockCases() []lockCase {
	w, f := models.PropertyTokenizationWord, models.PropertyTokenizationField
	text := func(filt, srch bool) func() *models.Property {
		return func() *models.Property {
			return &models.Property{Name: "p", DataType: []string{"text"}, Tokenization: w, IndexFilterable: boolPtr(filt), IndexSearchable: boolPtr(srch)}
		}
	}
	number := func() *models.Property {
		return &models.Property{Name: "p", DataType: []string{"int"}, IndexFilterable: boolPtr(true)}
	}
	return []lockCase{
		{"change-tokenization", "searchable", models.IndexUpsertRequest{Tokenization: f}, true, text(true, true), func(p *models.Property) { p.Tokenization = f }},
		{"change-tokenization-filterable", "filterable", models.IndexUpsertRequest{Tokenization: f}, true, text(true, false), func(p *models.Property) { p.Tokenization = f }},
		{"enable-searchable", "searchable", models.IndexUpsertRequest{Tokenization: w}, true, text(true, false), func(p *models.Property) {
			p.IndexSearchable, p.SearchableBlockmax = boolPtr(true), boolPtr(true)
		}},
		{"enable-filterable", "filterable", models.IndexUpsertRequest{}, true, text(false, true), func(p *models.Property) { p.IndexFilterable = boolPtr(true) }},
		{"enable-rangeable", "rangeable", models.IndexUpsertRequest{}, true, number, func(p *models.Property) { p.IndexRangeFilters = boolPtr(true) }},
		{"change-algorithm", "searchable", models.IndexUpsertRequest{Algorithm: "blockmax"}, false, text(true, true), func(p *models.Property) { p.SearchableBlockmax = boolPtr(true) }},
	}
}

func stoppedRun(t *testing.T, status distributedtask.TaskStatus, plan upsertPlan, multiTenant bool, decided ...string) *distributedtask.Task {
	units := map[string]*distributedtask.Unit{"s1": {ID: "s1"}}
	if multiTenant {
		units = map[string]*distributedtask.Unit{"a": {ID: "a", GroupID: "a"}, "b": {ID: "b", GroupID: "b"}}
	}
	task := buildTask(t, "T1", status, db.ReindexTaskPayload{
		Collection: "C", Properties: []string{"p"}, MigrationType: plan.migrationType,
		TargetTokenization: plan.targetTok, BucketStrategy: plan.bucketStrategy,
	}, units)
	task.SwapDecidedGroups = map[string]time.Time{}
	for _, g := range decided {
		task.SwapDecidedGroups[g] = time.Time{}
	}
	return task
}

// requireNotLocked fails if the refusals name a request that does not start a run.
func requireNotLocked(t *testing.T, class *models.Class, prop *models.Property, indexType string, tasks []*distributedtask.Task) {
	t.Helper()
	_, err := (&indexesHandlers{}).resolveRebuildPlan(class, prop, indexType, tasks, true)
	if !errors.Is(err, errRebuildPropertyPartlyMigrated) {
		return
	}
	m := namedResubmit.FindStringSubmatch(err.Error())
	require.NotNil(t, m, err.Error())
	named, ok := normalizeIndexTypeParam(m[2])
	require.True(t, ok)
	var body models.IndexUpsertRequest
	require.NoError(t, json.Unmarshal([]byte(m[3]), &body))
	plan, err := resolveUpsertPlan(class, "C", prop, named, &body, tasks, true)
	require.NoError(t, err)
	require.True(t, plan.conflict == "" && !plan.noop && plan.migrationType != "",
		"locked: the refusals name %q, which answers conflict=%q noop=%v", m[0], plan.conflict, plan.noop)
}

func newLockClass(tc lockCase, prop *models.Property) *models.Class {
	return &models.Class{Class: "C", Properties: []*models.Property{prop}, InvertedIndexConfig: &models.InvertedIndexConfig{UsingBlockMaxWAND: tc.flag}}
}

// Pins that a run whose completion write landed though it reported an error
// does not lock its property: the schema holds the run's own write, so it finished.
func TestALandedRunDoesNotLockItsProperty(t *testing.T) {
	for _, tc := range lockCases() {
		for _, multiTenant := range []bool{false, true} {
			decided := map[bool][]string{false: {""}, true: {"a", "b"}}[multiTenant]
			t.Run(tc.name+map[bool]string{false: "/single-tenant", true: "/multi-tenant"}[multiTenant], func(t *testing.T) {
				prop := tc.prop()
				class := newLockClass(tc, prop)
				req := tc.req
				plan, err := resolveUpsertPlan(class, "C", prop, tc.indexType, &req, nil, true)
				require.NoError(t, err)
				tasks := []*distributedtask.Task{stoppedRun(t, distributedtask.TaskStatusFailed, plan, multiTenant, decided...)}
				tc.toTarget(prop)
				requireNotLocked(t, class, prop, tc.indexType, tasks)
			})
		}
	}
}

// Pins that a class update, which turns the BlockMax flag on, does not lock a
// stopped change-algorithm run: the flag is not the run's own write.
func TestAClassUpdateDoesNotLockAPartialChangeAlgorithmRun(t *testing.T) {
	tc := lockCases()[5]
	for _, status := range []distributedtask.TaskStatus{distributedtask.TaskStatusCancelled, distributedtask.TaskStatusFailed} {
		for _, multiTenant := range []bool{false, true} {
			decided := map[bool][]string{false: {""}, true: {"a"}}[multiTenant]
			t.Run(string(status)+map[bool]string{false: "/single-tenant", true: "/multi-tenant"}[multiTenant], func(t *testing.T) {
				prop := tc.prop()
				class := newLockClass(tc, prop)
				req := tc.req
				plan, err := resolveUpsertPlan(class, "C", prop, "searchable", &req, nil, true)
				require.NoError(t, err)
				tasks := []*distributedtask.Task{stoppedRun(t, status, plan, multiTenant, decided...)}
				class.InvertedIndexConfig.UsingBlockMaxWAND = true
				requireNotLocked(t, class, prop, "searchable", tasks)
			})
		}
	}
}
