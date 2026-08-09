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

package reindex_mt

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/helper"
)

// TestMultiTenant_RepairRangeable_PerTenantSubset pins that
// repair-rangeable is a format-only migration.
//
// A migration is semantic if and only if it requires a global schema flip.
// enable-rangeable qualifies — it flips indexRangeFilters. repair-rangeable
// does not: it rebuilds an index that is already enabled, into a parallel
// bucket that is atomically swapped in per shard, and it writes no schema.
//
// That distinction is not academic. The semantic classification carries a
// 400 on the `tenants` parameter, because one global switch cannot be
// flipped for a subset of tenants. repair has no switch, so it keeps
// per-tenant targeting — and per-tenant targeting is what makes it usable
// as the recovery tool for clusters damaged by
// weaviate/0-weaviate-issues#464: repairing 20 tenants out of 50,000 must
// not require rebuilding all 50,000.
func TestMultiTenant_RepairRangeable_PerTenantSubset(t *testing.T) {
	ctx := context.Background()

	compose, err := reindexhelpers.StartSingleNode(ctx)
	require.NoError(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("terminate test containers: %s", err)
		}
	}()

	restURI := compose.GetWeaviate().URI()
	helper.SetupClient(restURI)
	defer func() {
		if t.Failed() {
			dumpWeaviateLogs(ctx, t, compose.GetWeaviate().Container())
		}
	}()

	const (
		className        = "MTRepairRangeableSubset"
		objectsPerTenant = 100
		inBand           = 60
	)
	tenants := []string{"rp0", "rp1", "rp2", "rp3", "rp4", "rp5"}
	targeted := []string{"rp1", "rp3"}

	trueVal, falseVal := true, false
	createMTClass(t, className, []*models.Property{
		{Name: "name", DataType: []string{"text"}},
		{
			Name:              "score",
			DataType:          []string{"int"},
			IndexFilterable:   &trueVal,
			IndexRangeFilters: &falseVal,
		},
	})
	defer helper.DeleteClass(t, className)
	addTenants(t, className, tenants)

	for _, tenant := range tenants {
		objects := make([]*models.Object, 0, objectsPerTenant)
		for i := 0; i < objectsPerTenant; i++ {
			objects = append(objects, &models.Object{
				Class: className,
				Properties: map[string]interface{}{
					"name":  "item_" + strconv.Itoa(i),
					"score": float64(i),
				},
				Tenant: tenant,
			})
		}
		helper.CreateObjectsBatch(t, objects)
	}

	countAllTenants := func(phase string) {
		t.Helper()
		var wrong []string
		for _, tenant := range tenants {
			gql := fmt.Sprintf(`{
				Get {
					%s(where: {path:["score"], operator:GreaterThanEqual, valueInt:40},
					   tenant: %q, limit: %d) {
						_additional { id }
					}
				}
			}`, className, tenant, objectsPerTenant)
			if got := len(runGraphQLQuery(t, className, gql)); got != inBand {
				wrong = append(wrong, fmt.Sprintf("%s=%d", tenant, got))
			}
		}
		require.Empty(t, wrong, "%s: every tenant must return %d objects; wrong: %v", phase, inBand, wrong)
	}

	// Enable across every tenant first — enable IS semantic, so it cannot
	// be scoped and there is nothing to repair until it has run.
	enableID := reindexhelpers.SubmitIndexUpdate(t, restURI, className, "score",
		`{"rangeable":{"enabled":true}}`)
	reindexhelpers.AwaitReindexFinished(t, restURI, enableID, reindexhelpers.WithTimeout(5*time.Minute))
	countAllTenants("after enable")

	// Restart so the deferred ingest→canonical renames land. Every tenant
	// shard then sits at the plain canonical name, which makes the repair's
	// per-shard blast radius directly observable below.
	require.NoError(t, compose.StopAt(ctx, 0, nil))
	require.NoError(t, compose.StartAt(ctx, 0))
	restURI = compose.GetWeaviate().URI()
	helper.SetupClient(restURI)

	for _, tenant := range tenants {
		dirs := listShardLSMDirs(ctx, t, compose.GetWeaviate().Container(), className, tenant)
		require.Contains(t, dirs, "property_score_rangeable",
			"tenant %s: post-restart the rangeable bucket must sit at its canonical name", tenant)
	}

	schemaBefore := rangeableSchemaSnapshot(t, restURI, className)

	// The core assertion: a tenant subset is ACCEPTED. Before
	// repair-rangeable was reclassified as format-only this returned 400
	// ("tenants parameter cannot be used with semantic migrations").
	repairID := reindexhelpers.SubmitIndexUpdate(t, restURI, className, "score",
		`{"rangeable":{"rebuild":true}}`, reindexhelpers.WithTenants(targeted))
	reindexhelpers.AwaitReindexFinished(t, restURI, repairID, reindexhelpers.WithTimeout(5*time.Minute))

	// Blast radius: only the targeted tenants were rebuilt. A rebuilt shard
	// leaves its live bucket under the ingest name until the next startup;
	// an untouched shard still sits at the canonical name.
	targetedSet := map[string]struct{}{}
	for _, tenant := range targeted {
		targetedSet[tenant] = struct{}{}
	}
	for _, tenant := range tenants {
		dirs := listShardLSMDirs(ctx, t, compose.GetWeaviate().Container(), className, tenant)
		rebuilt := false
		for _, dir := range dirs {
			if strings.HasPrefix(dir, "property_score_rangeable__rangeable_ingest") {
				rebuilt = true
			}
		}
		if _, want := targetedSet[tenant]; want {
			assert.Truef(t, rebuilt, "tenant %s was targeted and must have been rebuilt, dirs=%v", tenant, dirs)
		} else {
			assert.Falsef(t, rebuilt, "tenant %s was NOT targeted and must be untouched, dirs=%v", tenant, dirs)
			assert.Containsf(t, dirs, "property_score_rangeable",
				"tenant %s: untargeted shard must keep its canonical bucket", tenant)
		}
	}

	// repair writes no schema. Not just "the flag is still true" — no field
	// of the property may move, because a format-only migration by
	// definition commits nothing to RAFT.
	require.Equal(t, schemaBefore, rangeableSchemaSnapshot(t, restURI, className),
		"repair-rangeable must not mutate the schema; it is format-only by definition")

	countAllTenants("after per-tenant repair")
}

// rangeableSchemaSnapshot serialises the `score` property so a repair run
// can be asserted to have left every field of it untouched.
func rangeableSchemaSnapshot(t *testing.T, restURI, className string) string {
	t.Helper()
	cls, ok := reindexhelpers.FetchClass(restURI, className, false)
	require.True(t, ok, "class %s must be readable", className)
	for _, prop := range cls.Properties {
		if prop.Name == "score" {
			raw, err := json.Marshal(prop)
			require.NoError(t, err)
			return string(raw)
		}
	}
	t.Fatalf("property score not found on class %s", className)
	return ""
}
