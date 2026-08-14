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
	"fmt"
	"regexp"
	"slices"
	"testing"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
)

// TestPropertyGateMessagesAreByteIdenticalAcrossLayers pins that the
// pre-check and apply gate produce byte-identical refusals.
func TestPropertyGateMessagesAreByteIdenticalAcrossLayers(t *testing.T) {
	statuses := []distributedtask.TaskStatus{
		distributedtask.TaskStatusStarted,
		distributedtask.TaskStatusPreparing,
		distributedtask.TaskStatusSwapping,
		distributedtask.TaskStatus("VALIDATING"),
	}

	marshal := func(t *testing.T, p db.ReindexTaskPayload) []byte {
		t.Helper()
		payload, err := json.Marshal(p)
		require.NoError(t, err)
		return payload
	}

	payloads := []struct {
		name string
		// className is what both layers are called with — the qualified
		// form on a namespace-enabled cluster, matching the payload.
		className string
		build     func(*testing.T) []byte
		// wantInReason, when set, must appear in the shared refusal.
		wantInReason []string
		// notInReason, when set, must not.
		notInReason []string
	}{
		{
			name: "the property by name",
			build: func(t *testing.T) []byte {
				return marshal(t, db.ReindexTaskPayload{
					MigrationType: db.ReindexTypeChangeTokenization,
					Collection:    "C",
					Properties:    []string{"name"},
				})
			},
			wantInReason: []string{`POST /v1/schema/C/properties/name/index/searchable/cancel`},
		},
		{
			// The asked-for property is named, not the first one in the task.
			name: "the property among several",
			build: func(t *testing.T) []byte {
				return marshal(t, db.ReindexTaskPayload{
					MigrationType: db.ReindexTypeChangeTokenization,
					Collection:    "C",
					Properties:    []string{"other", "name"},
				})
			},
			wantInReason: []string{`POST /v1/schema/C/properties/name/index/searchable/cancel`},
			notInReason:  []string{"/properties/other/"},
		},
		{
			// Empty Properties means "all properties" via different code on
			// each side.
			name: "no properties means all of them",
			build: func(t *testing.T) []byte {
				return marshal(t, db.ReindexTaskPayload{
					MigrationType: db.ReindexTypeChangeTokenization,
					Collection:    "C",
				})
			},
		},
		{
			name: "a payload written by an older binary",
			build: func(t *testing.T) []byte {
				return marshal(t, db.ReindexTaskPayload{Collection: "C", Properties: []string{"name"}})
			},
		},
		{
			name:  "a payload that will not parse",
			build: func(t *testing.T) []byte { return []byte("{not json") },
		},
		{
			// Both layers keep the qualified class in the cancel URL.
			name:      "a namespace-qualified collection",
			className: "customer1:C",
			build: func(t *testing.T) []byte {
				return marshal(t, db.ReindexTaskPayload{
					MigrationType:      db.ReindexTypeChangeTokenization,
					Collection:         "customer1:C",
					Properties:         []string{"name"},
					TargetTokenization: "word",
				})
			},
			wantInReason: []string{`POST /v1/schema/customer1:C/properties/name/index/searchable/cancel`},
			notInReason:  []string{"/v1/schema/C/"},
		},
	}

	for _, p := range payloads {
		for _, status := range statuses {
			t.Run(p.name+"/"+string(status), func(t *testing.T) {
				className := p.className
				if className == "" {
					className = "C"
				}
				task := &distributedtask.Task{
					Namespace:      db.ReindexNamespace,
					TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_remedy"},
					Status:         status,
					Payload:        p.build(t),
				}

				h := &schemaHandlers{
					reindexTaskLister: fakeReindexTaskLister{tasks: map[string][]*distributedtask.Task{
						db.ReindexNamespace: {task},
					}},
				}
				preCheck := h.checkReindexConflictForPropertyMutation(context.Background(), className, "name")

				applyGate := (&db.ReindexProvider{}).CheckPropertyUpdate(
					className, "name", []*distributedtask.Task{task})
				require.Error(t, applyGate)

				require.Equal(t, applyGate.Error(), preCheck)
				// wantInReason is the rendered cancel call, which both layers
				// print only in the status the cancel endpoint accepts one in.
				// Asserted in both directions off the endpoint's own predicate,
				// so neither layer can start advertising a cancel that 409s.
				for _, want := range p.wantInReason {
					if status.IsCancellable() {
						require.Contains(t, preCheck, want)
					} else {
						require.NotContains(t, preCheck, want)
					}
				}
				for _, unwanted := range p.notInReason {
					require.NotContains(t, preCheck, unwanted)
				}
			})
		}
	}
}

// TestTheCancelHelpersReadTheSharedIndexTypeMapping pins that both REST
// helpers delegate to db.ReindexTargetIndexes rather than forking their own copy.
func TestTheCancelHelpersReadTheSharedIndexTypeMapping(t *testing.T) {
	migrationTypes := []db.ReindexMigrationType{
		db.ReindexTypeChangeTokenization, // two index types
		db.ReindexTypeEnableRangeable,    // one
		db.ReindexMigrationType("invent-index"),
	}

	for _, mt := range migrationTypes {
		t.Run(string(mt), func(t *testing.T) {
			targets := db.ReindexTargetIndexes(mt)

			indexTypes, known := indexTypesFromMigrationType(mt)
			require.Equal(t, targets, indexTypes)
			require.Equal(t, len(targets) > 0, known)

			for _, indexType := range []string{"searchable", "filterable", "rangeable"} {
				matches, known := migrationTypeTargetsIndex(mt, indexType)
				require.Equal(t, len(targets) > 0, known)
				require.Equal(t, slices.Contains(targets, indexType), matches)
			}
		})
	}
}

// TestTheRenderedCallReachesBothKindsOfCaller pins that the rendered URL
// keeps its namespace prefix for a global operator, but is stripped for a
// namespace-confined caller.
func TestTheRenderedCallReachesBothKindsOfCaller(t *testing.T) {
	payload := db.ReindexTaskPayload{
		MigrationType: db.ReindexTypeChangeTokenization,
		Collection:    "customer1:C",
		Properties:    []string{"name"},
	}
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	task := &distributedtask.Task{
		Namespace:      db.ReindexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_remedy"},
		Status:         distributedtask.TaskStatusStarted,
		Payload:        raw,
	}
	h := &schemaHandlers{
		reindexTaskLister: fakeReindexTaskLister{tasks: map[string][]*distributedtask.Task{
			db.ReindexNamespace: {task},
		}},
	}
	reason := h.checkReindexConflictForPropertyMutation(context.Background(), "customer1:C", "name")
	require.NotEmpty(t, reason)

	cases := []struct {
		name      string
		principal *models.Principal
		want      string
	}{
		{
			name:      "namespace-confined caller",
			principal: &models.Principal{Namespace: "customer1"},
			want:      `POST /v1/schema/C/properties/name/index/searchable/cancel`,
		},
		{
			name:      "global operator",
			principal: &models.Principal{Namespace: "customer1", IsGlobalOperator: true},
			want:      `POST /v1/schema/customer1:C/properties/name/index/searchable/cancel`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload := errPayloadFromSingleErr(tc.principal, fmt.Errorf("%s", reason))
			require.NotNil(t, payload)
			require.Len(t, payload.Error, 1)
			require.Contains(t, payload.Error[0].Message, tc.want)
		})
	}
}

// declaredReindexMigrationTypes is every migration type this build
// declares, mirrored here by hand: db.TestReindexTargetIndexes catches a
// new type growing past its own count, but points at that failure, not
// this file, and a stale mirror here goes undetected. Keeping it current
// is manual.
var declaredReindexMigrationTypes = []db.ReindexMigrationType{
	db.ReindexTypeChangeTokenization,
	db.ReindexTypeChangeTokenizationFilterable,
	db.ReindexTypeEnableFilterable,
	db.ReindexTypeEnableSearchable,
	db.ReindexTypeEnableRangeable,
	db.ReindexTypeRepairFilterable,
	db.ReindexTypeRepairRangeable,
	db.ReindexTypeRebuildSearchable,
	db.ReindexTypeChangeAlgorithm,
}

// renderedCallRE matches a call the reindex messages render. The index type
// and the verb both ride the path now, and the body (upserts only) is a flat
// IndexUpsertRequest, so the groups are:
// collection, property, indexName, verb suffix, body.
var renderedCallRE = regexp.MustCompile(
	`(?:PUT|POST) /v1/schema/([^/ ]+)/properties/([^/ ]+)/index/([A-Za-z]+)(/rebuild|/cancel)?(?:\?tenants=[^ ]*)?(?: -d '(\{[^']*\})')?`)

// renderedCall is one call parsed out of a rendered message.
type renderedCall struct {
	whole      string
	collection string
	property   string
	indexType  string // internal token, folded from the path spelling
	isCancel   bool
	isRebuild  bool
	body       models.IndexUpsertRequest
}

// parseRenderedCalls pulls every call a message names. It fails the test on a
// path the router would 422 or a body the API cannot parse, so a remedy that
// prints an unreachable request never reads as a pass.
func parseRenderedCalls(t *testing.T, message string) []renderedCall {
	t.Helper()
	var out []renderedCall
	for _, m := range renderedCallRE.FindAllStringSubmatch(message, -1) {
		indexType, ok := normalizeIndexTypeParam(m[3])
		require.True(t, ok,
			"the remedy rendered index type %q, which the route enum rejects with 422: %s", m[3], m[0])
		c := renderedCall{
			whole: m[0], collection: m[1], property: m[2], indexType: indexType,
			isCancel: m[4] == "/cancel", isRebuild: m[4] == "/rebuild",
		}
		if m[5] != "" {
			require.NoError(t, json.Unmarshal([]byte(m[5]), &c.body),
				"the remedy rendered a body the API cannot parse: %s", m[5])
		}
		out = append(out, c)
	}
	return out
}

// acceptRenderedCall runs a rendered non-cancel call through the same resolver
// the live handler runs, so the assertion is "the server would accept this",
// not "it matches a string we also wrote".
func acceptRenderedCall(h *indexesHandlers, class *models.Class, prop *models.Property, c renderedCall) error {
	if c.isRebuild {
		_, err := h.resolveRebuildPlan(class, prop, c.indexType, nil)
		return err
	}
	switch c.indexType {
	case "searchable":
		_, err := resolveSearchableUpsert(class, c.collection, prop, c.body.Tokenization, c.body.Algorithm, nil)
		return err
	case "filterable":
		_, err := resolveFilterableUpsert(c.collection, prop, c.body.Tokenization, c.body.Algorithm, nil)
		return err
	default:
		_, err := resolveRangeableUpsert(class, c.collection, prop, c.body.Tokenization, c.body.Algorithm, nil)
		return err
	}
}

// Drives the cancel call a remedy prints through the real cancel path
// (findCancelTarget, cancelPreflight) and fails if the API would refuse it
// — the check a Contains-based string table can't make. Runs both ways: a
// printed cancel must be accepted, and an acceptable status must print one.
func TestGateRemedyNamesOnlyACancelTheCancelPathAccepts(t *testing.T) {
	require.Len(t, declaredReindexMigrationTypes, 9,
		"a new migration type needs rows here as well")

	statuses := []distributedtask.TaskStatus{
		distributedtask.TaskStatusStarted,
		distributedtask.TaskStatusPreparing,
		distributedtask.TaskStatusSwapping,
		unknownFutureStatus,
	}

	for _, migrationType := range declaredReindexMigrationTypes {
		for _, status := range statuses {
			for _, dropsTheData := range []bool{false, true} {
				name := fmt.Sprintf("%s/%s/dropsTheData=%v", migrationType, status, dropsTheData)
				t.Run(name, func(t *testing.T) {
					payload := db.ReindexTaskPayload{
						Collection:         "C",
						MigrationType:      migrationType,
						Properties:         []string{"name"},
						TargetTokenization: "word",
					}
					remedy := db.ReindexGateRemedy(status, payload, "name", dropsTheData, false)
					require.NotEmpty(t, remedy)

					logger, _ := logrustest.NewNullLogger()
					h := &indexesHandlers{appState: &state.State{Logger: logger}}
					task := buildTask(t, "T1", status, payload, nil)

					var printed int
					for _, c := range parseRenderedCalls(t, remedy) {
						if !c.isCancel {
							continue
						}
						printed++

						target, _ := findCancelTarget(
							[]*distributedtask.Task{task}, c.collection, c.property, c.indexType, logger)
						require.NotNil(t, target,
							"the remedy named a cancel target the matcher does not find: %s", c.whole)
						require.Nil(t,
							h.cancelPreflight(target, c.collection, c.property, c.indexType, nil),
							"the remedy named a cancel the API refuses in status %s: %s", status, c.whole)
					}

					if status.IsCancellable() && db.ReindexCancelCall(payload, "name") != "" {
						require.Equal(t, 1, printed,
							"a cancel is accepted in status %s and this build can name it, "+
								"so the remedy has to print it exactly once", status)
						return
					}
					require.Zero(t, printed,
						"no cancel is accepted in status %s, so the remedy must print none", status)
				})
			}
		}
	}
}

// TestEveryRenderedRepairCallPassesTheRealValidator checks that every
// rendered repair/cancel call also passes the real per-type validator, not
// just group exclusivity. Not every declared type renders one: the table's
// wantNoRepair rows pin the types that deliberately do not.
func TestEveryRenderedRepairCallPassesTheRealValidator(t *testing.T) {
	const targetTok = "word"
	// The tokenization every retokenize row is migrating away from.
	const oldTok = "whitespace"

	enabled, disabled := true, false
	textProp := func(name string, searchable, filterable *bool, tok string) *models.Property {
		return &models.Property{
			Name: name, DataType: []string{"text"},
			IndexSearchable: searchable, IndexFilterable: filterable,
			Tokenization: tok,
		}
	}
	numericProp := func(name string, rangeable *bool) *models.Property {
		return &models.Property{Name: name, DataType: []string{"int"}, IndexRangeFilters: rangeable}
	}

	// Every row drives the rendered call through the live resolver
	// (acceptRenderedCall), so a type needs no validator of its own here.
	cases := []struct {
		migrationType db.ReindexMigrationType
		prop          *models.Property
		usingBlockMax bool
		// wantNoRepair marks a type that deliberately renders no repair call.
		wantNoRepair string
	}{
		{
			migrationType: db.ReindexTypeChangeAlgorithm,
			prop:          textProp("name", &enabled, nil, targetTok),
		},
		{
			migrationType: db.ReindexTypeRebuildSearchable,
			prop:          textProp("name", &enabled, nil, targetTok),
			usingBlockMax: true,
		},
		{
			migrationType: db.ReindexTypeRepairFilterable,
			prop:          textProp("name", nil, &enabled, targetTok),
		},
		{
			migrationType: db.ReindexTypeEnableRangeable,
			prop:          numericProp("name", &disabled),
			// The flag flips per shard as the migration runs, so no single
			// verb is accepted in every terminal state.
			wantNoRepair: "enable-rangeable has no repairable terminal state",
		},
		{
			migrationType: db.ReindexTypeRepairRangeable,
			prop:          numericProp("name", &enabled),
		},
		{
			migrationType: db.ReindexTypeEnableFilterable,
			prop:          textProp("name", nil, &disabled, targetTok),
		},
		{
			migrationType: db.ReindexTypeEnableSearchable,
			prop:          textProp("name", &disabled, &disabled, ""),
		},
		{
			migrationType: db.ReindexTypeChangeTokenization,
			prop:          textProp("name", &enabled, nil, oldTok),
		},
		{
			migrationType: db.ReindexTypeChangeTokenizationFilterable,
			prop:          textProp("name", nil, &enabled, oldTok),
		},
	}

	for _, tc := range cases {
		t.Run(string(tc.migrationType), func(t *testing.T) {
			payload := db.ReindexTaskPayload{
				Collection:         "C",
				MigrationType:      tc.migrationType,
				Properties:         []string{"name"},
				TargetTokenization: targetTok,
			}
			class := &models.Class{
				Class:      "C",
				Properties: []*models.Property{tc.prop},
				InvertedIndexConfig: &models.InvertedIndexConfig{
					UsingBlockMaxWAND: tc.usingBlockMax,
				},
			}

			logger, _ := logrustest.NewNullLogger()
			h := &indexesHandlers{appState: &state.State{Logger: logger}}

			repair := db.ReindexRepairCall(payload, "name")
			if tc.wantNoRepair != "" {
				require.Empty(t, repair, tc.wantNoRepair)
			} else {
				require.NotEmpty(t, repair, "every other declared type needs a repair call")
				calls := parseRenderedCalls(t, repair)
				require.Len(t, calls, 1, "a repair call renders exactly one request")
				require.False(t, calls[0].isCancel, "the repair call must not be a cancel")
				require.NoError(t, acceptRenderedCall(h, class, tc.prop, calls[0]),
					"the rendered repair call is rejected by the resolver that receives it")
			}

			// Cancel skips the per-type preconditions, but not
			// cancelPreflight, whose IsCancellable check refuses everything
			// past STARTED. So the rendered call goes through the matcher and
			// the pre-flight, not just through a shape check.
			cancel := db.ReindexCancelCall(payload, "name")
			require.NotEmpty(t, cancel, "every declared type needs a cancel call")
			cancelCalls := parseRenderedCalls(t, cancel)
			require.Len(t, cancelCalls, 1)
			require.True(t, cancelCalls[0].isCancel, "the rendered cancel call must read as one")
			indexType := cancelCalls[0].indexType

			task := buildTask(t, "T1", distributedtask.TaskStatusStarted, payload, nil)
			target, _ := findCancelTarget(
				[]*distributedtask.Task{task}, "C", "name", indexType, logger)
			require.NotNil(t, target, "the rendered cancel call must find its target")
			require.Nil(t, h.cancelPreflight(target, "C", "name", indexType, nil),
				"the rendered cancel call is refused by the pre-flight that receives it")
		})
	}

	// Ceiling, stated plainly: this fires only once someone has already added
	// the new type to the declaredReindexMigrationTypes mirror above. A type
	// declared in db and never mirrored here goes unnoticed, and closing that
	// would take an exported registry in db or a source scan of it from here.
	t.Run("the table covers every declared type", func(t *testing.T) {
		covered := map[db.ReindexMigrationType]bool{}
		for _, tc := range cases {
			covered[tc.migrationType] = true
		}
		for _, mt := range declaredReindexMigrationTypes {
			require.True(t, covered[mt],
				"migration type %q is declared but not pinned here", mt)
		}
		require.Len(t, cases, len(declaredReindexMigrationTypes), "no obsolete rows")
	})
}

// TestBothLayersNameTheSameTaskWhenSeveralConflict pins that the pre-check
// names the same conflicting task as the apply gate, and that the
// pre-check's answer holds regardless of the task list's input order. The
// apply gate itself takes the first overlap in slice order; its
// order-independence is sortedTasksWithLock's job one layer up, so this
// test hands it the already-sorted slice production hands it.
func TestBothLayersNameTheSameTaskWhenSeveralConflict(t *testing.T) {
	payload, err := json.Marshal(db.ReindexTaskPayload{
		MigrationType:      db.ReindexTypeChangeTokenization,
		Collection:         "C",
		Properties:         []string{"name"},
		TargetTokenization: "word",
	})
	require.NoError(t, err)

	task := func(id string) *distributedtask.Task {
		return &distributedtask.Task{
			Namespace:      db.ReindexNamespace,
			TaskDescriptor: distributedtask.TaskDescriptor{ID: id},
			Status:         distributedtask.TaskStatusStarted,
			Payload:        payload,
		}
	}
	// All three conflict on C.name; only the ordering differs. Three, not
	// two, so that reversing the insertion order does not happen to sort it.
	sorted := []*distributedtask.Task{task("T_a"), task("T_b"), task("T_c")}
	unsorted := []*distributedtask.Task{task("T_b"), task("T_a"), task("T_c")}

	h := &schemaHandlers{
		reindexTaskLister: fakeReindexTaskLister{tasks: map[string][]*distributedtask.Task{
			db.ReindexNamespace: unsorted,
		}},
	}
	preCheck := h.checkReindexConflictForPropertyMutation(context.Background(), "C", "name")

	applyGate := (&db.ReindexProvider{}).CheckPropertyUpdate("C", "name", sorted)
	require.Error(t, applyGate)

	require.Equal(t, applyGate.Error(), preCheck)
	require.Contains(t, preCheck, `"T_a"`)
	require.NotContains(t, preCheck, `"T_b"`)
	require.NotContains(t, preCheck, `"T_c"`)
}
