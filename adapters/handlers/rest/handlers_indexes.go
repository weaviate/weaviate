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
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-openapi/runtime"
	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/clients"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/logrusext"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

func setupIndexesHandlers(api *operations.WeaviateAPI, appState *state.State) {
	h := newIndexesHandlers(appState)
	api.SchemaSchemaObjectsIndexesGetHandler = schema.SchemaObjectsIndexesGetHandlerFunc(h.getIndexes)
	api.SchemaSchemaObjectsIndexesUpdateHandler = schema.SchemaObjectsIndexesUpdateHandlerFunc(h.updateIndex)
}

// newIndexesHandlers wires the collaborators every gate in this file consults.
// A collaborator left nil here disables the gate that reads it, so the wiring
// is behavior and is tested as such. It is split out of setupIndexesHandlers
// so a test can build the handlers without a swagger API.
func newIndexesHandlers(appState *state.State) *indexesHandlers {
	h := &indexesHandlers{appState: appState}
	if appState.Cluster != nil {
		h.cluster = appState.Cluster
	}
	if appState.ClusterService != nil {
		h.tasks = appState.ClusterService
	}
	if appState.ClusterHttpClient != nil && appState.Cluster != nil && appState.ServerConfig != nil {
		// Not the shared cluster client: these two probes must reach the peer
		// itself. See [reindexGateProbeHttpClient].
		probeClient := reindexGateProbeHttpClient(
			appState.ServerConfig.Config.Cluster.AuthConfig,
			appState.ServerConfig.Config.MinimumInternalTimeout,
		)
		h.backupActivity = clients.NewClusterBackupActivity(probeClient, appState.Cluster)
		h.reindexCleanup = clients.NewClusterReindexCleanup(probeClient, appState.Cluster)
	}
	if appState.BackupActivity != nil {
		h.localBackupActivity = appState.BackupActivity
	}
	return h
}

type indexesHandlers struct {
	appState *state.State

	// nil in fixtures without a cluster HTTP client; probeBackupActivity allows submission then.
	backupActivity nodeActivityProber

	// nil in fixtures without a backup manager; the submit-gate pre-check is
	// skipped then and the fan-out probe is the only backup check.
	localBackupActivity localActivityProber

	// nil in fixtures without a cluster; treated the same as an unwired probe.
	cluster clusterMembership

	// nil until wired; both reindex routes answer 503 then.
	tasks reindexTaskService

	// nil in fixtures without a cluster HTTP client; the cancel handler then
	// answers without confirming remote gates.
	reindexCleanup reindexCleanupProber

	// Per-handler, not per-process: a package-level budget leaves every test
	// after the first with an exhausted one. See [backupActivityGateWarn].
	gateWarnOnce    sync.Once
	gateWarnSampler *logrusext.Sampler
}

// backupActivityGateWarn rate-limits the fail-open WARN to one line per hour.
// The condition it reports — no probe wired, or an empty node list — is a
// persistent misconfiguration, so a once-per-process line would leave every
// later submission silent for an operator who starts reading the logs after the
// first one. Built on first use because fixtures construct the handler directly.
func (h *indexesHandlers) backupActivityGateWarn() *logrusext.Sampler {
	h.gateWarnOnce.Do(func() {
		h.gateWarnSampler = logrusext.NewSampler(logrus.StandardLogger(), 1, time.Hour)
	})
	return h.gateWarnSampler
}

// clusterMembership is the slice of the cluster state the backup gate needs.
type clusterMembership interface {
	AllNames() []string
	LocalName() string
}

// localActivityProber reads this node's own backup and restore slots in
// process. It gives the same answer the fan-out probe would get for this one
// node, without leaving the process.
type localActivityProber interface {
	Activity() backup.NodeActivity
}

// reindexCleanupProber asks one node whether its reindex-cleanup gate is closed.
type reindexCleanupProber interface {
	CleanupInProgress(ctx context.Context, nodeName, collection string) (bool, error)
}

// reindexTaskService is a narrow port over the four cluster-service methods the
// reindex admission path uses; it exists because submission has to interleave
// task writes with cluster-wide backup probes to settle the admission race
// between the two, and neither that ordering nor the call sites around it can
// be covered against a live RAFT node.
type reindexTaskService interface {
	ListDistributedTasks(ctx context.Context) (map[string][]*distributedtask.Task, error)
	CancelDistributedTask(ctx context.Context, namespace, taskID string, taskVersion uint64) error
	AddDistributedTaskWithBarrier(ctx context.Context, namespace, taskID string,
		taskPayload any, unitIDs []string, needsPreparationBarrier bool) error
	AddDistributedTaskWithGroupsBarrier(ctx context.Context, namespace, taskID string,
		taskPayload any, unitSpecs []distributedtask.UnitSpec, needsPreparationBarrier bool) error
}

// submitLock returns the per-(collection, property) mutex for the
// check-and-submit critical section, allocating one on first use.
//
// The actual lock manager lives on appState (ReindexSubmitLocks) so
// it is SHARED with the DELETE-property-index REST handler. Without
// the sharing, a parallel PUT /indexes/{prop} (which submits a
// reindex task) and DELETE /properties/{prop}/index/{indexName}
// (which drops the canonical bucket) race at the RAFT serializer and
// produce a torn bucket — see [state.ReindexSubmitLocks] godoc for the
// full failure shape.
//
// The map is keyed by collection-lowercased + property so case-folded
// collection lookups (matching the rest of the conflict logic) hit
// the same lock entry.
func (h *indexesHandlers) submitLock(collection, propertyName string) *sync.Mutex {
	return h.appState.ReindexSubmitLocks.SubmitLockFor(collection, propertyName)
}

// getIndexes implements GET /v1/schema/{className}/indexes.
func (h *indexesHandlers) getIndexes(params schema.SchemaObjectsIndexesGetParams, principal *models.Principal) middleware.Responder {
	// Resolve (alias-aware) before authz so authz and the lookup use the qualified name.
	collection, _, rErr := namespacing.Resolve(principal, h.appState.SchemaManager,
		h.appState.ServerConfig.Config.Namespaces.Enabled, params.ClassName)
	if rErr != nil {
		return schema.NewSchemaObjectsIndexesGetForbidden().WithPayload(errPayloadFromSingleErr(principal, rErr))
	}

	// Require READ on the collection's metadata: this endpoint exposes
	// per-property index state, which is collection-internal information.
	if err := h.appState.Authorizer.Authorize(params.HTTPRequest.Context(), principal,
		authorization.READ, authorization.CollectionsMetadata(collection)...); err != nil {
		if errors.As(err, &authzerrors.Forbidden{}) {
			return schema.NewSchemaObjectsIndexesGetForbidden().WithPayload(errPayloadFromSingleErr(principal, err))
		}
		return schema.NewSchemaObjectsIndexesGetInternalServerError().WithPayload(errPayloadFromSingleErr(principal, err))
	}

	class := h.appState.SchemaManager.ReadOnlyClass(collection)
	if class == nil {
		return schema.NewSchemaObjectsIndexesGetNotFound()
	}

	// Fetch active reindex tasks.
	var activeTasks map[string][]*distributedtask.Task
	if h.tasks != nil {
		var err error
		activeTasks, err = h.tasks.ListDistributedTasks(context.Background())
		if err != nil {
			activeTasks = nil // degrade gracefully
		}
	}

	// Pre-parse the reindex task payloads once per request so the per-property
	// merge below doesn't re-unmarshal each task N times.
	parsedTasks := parseReindexTasks(activeTasks[db.ReindexNamespace])

	// finalizeWindow bounds the "FINISHED but flag-off → indexing@100%"
	// override in mergeReindexStatus. The legitimate window is at most
	// one DTM scheduler tick (the gap between task FINISHED and the
	// scheduler calling OnGroupCompleted) plus the per-shard swap
	// duration (typically <1s). We use 2× the tick interval as a
	// generous coverage. The clamp at finalizeWindowMin/Max keeps the
	// window reasonable in both pathological sub-second tick configs
	// (clamp up to 3s) and production 60s+ tick configs (clamp down to
	// 10s) — a longer-lived bleed in production was the user-visible
	// face of https://github.com/weaviate/weaviate/issues/10675, and capping the override here
	// keeps the worst-case stale "indexing(1)" pill bounded.
	finalizeWindow := 2 * h.appState.ServerConfig.Config.DistributedTasks.SchedulerTickInterval
	if finalizeWindow < finalizeWindowMin {
		finalizeWindow = finalizeWindowMin
	}
	if finalizeWindow > finalizeWindowMax {
		finalizeWindow = finalizeWindowMax
	}

	// UsingBlockMaxWAND flips cluster-wide only after every searchable
	// bucket on every shard is blockmax; mid-flight, targetAlgorithm
	// (set by mergeReindexStatus) carries the "incoming" signal.
	searchableAlgorithm := models.IndexStatusAlgorithmWand
	if class.InvertedIndexConfig != nil && class.InvertedIndexConfig.UsingBlockMaxWAND {
		searchableAlgorithm = models.IndexStatusAlgorithmBlockmax
	}

	// Build per-property index status.
	props := make([]*models.PropertyIndexStatus, 0, len(class.Properties))
	for _, prop := range class.Properties {
		pis := &models.PropertyIndexStatus{
			Name: prop.Name,
			// Reference DataTypes carry the qualified target class.
			DataType: namespacing.StripOwnNamespace(principal, dataTypeString(prop)),
		}
		pis.Description = prop.Description

		// One entry per applicable index type. carryTokenization mirrors
		// the historical behavior: filterable and searchable expose the
		// property's tokenization on the flag-on entry; rangeable does not.
		// Rangeable only applies to numeric/date properties.
		isNumeric := isNumericProperty(prop)
		entries := []struct {
			indexType         string
			flagOn            bool
			applicable        bool
			carryTokenization bool
		}{
			{"filterable", prop.IndexFilterable == nil || *prop.IndexFilterable, true, true},
			{"searchable", prop.IndexSearchable == nil || *prop.IndexSearchable, true, true},
			{"rangeable", prop.IndexRangeFilters != nil && *prop.IndexRangeFilters, isNumeric, false},
		}

		var indexes []*models.IndexStatus
		for _, e := range entries {
			if !e.applicable {
				continue
			}
			idx := &models.IndexStatus{Type: e.indexType, Status: "ready"}
			if e.flagOn && e.carryTokenization {
				idx.Tokenization = prop.Tokenization
			}
			// Only searchable indexes have a BM25 algorithm; surface the
			// class-level wand/blockmax state so the UI can render it
			// honestly. Filterable / rangeable have no equivalent today.
			if e.indexType == "searchable" && e.flagOn {
				idx.Algorithm = searchableAlgorithm
			}
			mergeReindexStatus(idx, collection, prop.Name, e.indexType, e.flagOn, parsedTasks, finalizeWindow, h.appState.Logger)
			// Flag on → always emit. Flag off → emit only when a reindex
			// task carries actionable signal (in-flight or terminal
			// failure/cancellation).
			if e.flagOn || isSyntheticStatus(idx.Status) {
				indexes = append(indexes, idx)
			}
		}

		pis.Indexes = indexes
		props = append(props, pis)
	}

	return schema.NewSchemaObjectsIndexesGetOK().WithPayload(&models.IndexStatusResponse{
		Collection: namespacing.StripOwnNamespace(principal, collection),
		Properties: props,
	})
}

// updateIndex implements PUT /v1/schema/{className}/indexes/{propertyName}.
//
// Concurrent non-conflicting reindex tasks are allowed. Two tasks conflict if
// they would touch the same bucket for the same property. The conflict check
// rejects same-type same-property tasks, plus cross-type conflicts (e.g.
// repair-searchable blocks change-tokenization on any property since
// repair-searchable touches all searchable buckets).
func (h *indexesHandlers) updateIndex(params schema.SchemaObjectsIndexesUpdateParams, principal *models.Principal) middleware.Responder {
	propertyName := params.PropertyName

	// Qualify (no alias resolution, like DeleteClassPropertyIndex) before authz + lookup.
	collection, qErr := namespacing.QualifyClass(principal,
		h.appState.ServerConfig.Config.Namespaces.Enabled, params.ClassName)
	if qErr != nil {
		return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errPayloadFromSingleErr(principal, qErr))
	}

	// Require UPDATE on the collection itself: submitting a reindex task is a
	// privileged, cluster-wide, destructive operation (rebuilds buckets on
	// every replica, flips schema flags). The read-only authzed sibling above
	// uses CollectionsMetadata; here we need the stronger Collections verb.
	if err := h.appState.Authorizer.Authorize(params.HTTPRequest.Context(), principal,
		authorization.UPDATE, authorization.Collections(collection)...); err != nil {
		if errors.As(err, &authzerrors.Forbidden{}) {
			return schema.NewSchemaObjectsIndexesUpdateForbidden().WithPayload(errPayloadFromSingleErr(principal, err))
		}
		return schema.NewSchemaObjectsIndexesUpdateInternalServerError().WithPayload(errPayloadFromSingleErr(principal, err))
	}

	// Refuse to START a reindex while the feature is off. Cancel and the
	// status endpoint are deliberately untouched, so a task that was
	// already running stays observable and stoppable. Placed after authz
	// so an unauthorized caller still gets its 401/403.
	if !h.appState.ServerConfig.Config.RuntimeReindexEnabled && !requestsCancel(params.Body) {
		return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(principal,
			"runtime reindex is disabled; enable with RUNTIME_REINDEX_ENABLED=true"))
	}

	// Acquire the per-(collection, property) submit lock EARLY — before
	// reading the class or running any validation — so a parallel DELETE
	// on /properties/{prop}/index/{name} cannot mutate the schema (drop
	// the canonical bucket) between this handler's class read and its
	// task-add RAFT call.
	//
	// The previous lock position (just before AddDistributedTask, after
	// validation) was insufficient: a parallel DELETE could win the lock,
	// flip IndexSearchable=false + drop the searchable bucket, release;
	// meanwhile PUT was already past its `class := ReadOnlyClass(...)` +
	// `validateTokenizationChange(targetProp)` snapshot which still
	// observed IndexSearchable=true, so validation passed and PUT
	// proceeded to submit a change-tok task against a no-longer-existing
	// bucket — FilterableRetokenize/SearchableRetokenize then failed
	// at the swap step. The
	// TestParallelConflictMatrix/change_tokenization_both__delete_searchable_parallel
	// case in test/acceptance/reindex_concurrent pins this scenario.
	//
	// Now: PUT holds the lock across class read + validation + RAFT
	// task-add. A concurrent DELETE waits; when it acquires, the task
	// is in-flight in RAFT and the apply-time MutationGuard
	// rejects the DELETE deterministically. If DELETE wins instead,
	// PUT's class read sees IndexSearchable=false and
	// validateTokenizationChange rejects with 400.
	// Key on the qualified class (the reindex-task key) so short- and qualified-name
	// callers for the same collection share the DeleteClassPropertyIndex lock.
	//
	// Worst case with one unreachable node the lock is held ~10s (two 5s backup
	// probes plus the RAFT task-add), during which a DELETE on the same property
	// waits and MarkSubmitInProgress refuses the collection's backups. The probes
	// are not hoisted out of the lock even though they read no schema state:
	// outside it they would run before the 404, conflict and cap checks, so every
	// request that fails locally would pay a cluster-wide fan-out first, and a
	// submit for a collection that does not exist would answer 409 instead of 404.
	//
	// Released through a OnceFunc rather than a bare deferred Unlock so the
	// rollback path can hand it back early and this defer still covers every
	// other return.
	propLock := h.submitLock(collection, propertyName)
	propLock.Lock()
	releaseSubmitLock := sync.OnceFunc(propLock.Unlock)
	defer releaseSubmitLock()

	class := h.appState.SchemaManager.ReadOnlyClass(collection)
	if class == nil {
		return schema.NewSchemaObjectsIndexesUpdateNotFound().WithPayload(
			errorResponse(principal, fmt.Sprintf("collection %q not found", collection)),
		)
	}

	// Find the property.
	var targetProp *models.Property
	for _, p := range class.Properties {
		if p.Name == propertyName {
			targetProp = p
			break
		}
	}
	if targetProp == nil {
		return schema.NewSchemaObjectsIndexesUpdateNotFound().WithPayload(
			errorResponse(principal, fmt.Sprintf("property %q not found on collection %q", propertyName, collection)),
		)
	}

	body := params.Body
	if body == nil {
		return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(principal, "request body required"))
	}

	// Reject ambiguous bodies (multiple groups set, conflicting verbs within
	// a group, or zero verbs) before the switch silently picks one arm.
	if err := validateBodyExclusivity(body); err != nil {
		return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(principal, err.Error()))
	}

	// Cancel is fundamentally different from the other actions: it does not
	// submit a new task, it asks DTM to abort one. Handle it up front so the
	// switch below stays focused on submit-shaped intents.
	if cancelIndexType, cancelling := requestedCancel(body); cancelling {
		return h.cancelReindexTask(params.HTTPRequest.Context(), collection, propertyName, cancelIndexType, principal)
	}

	// Determine which migration type to submit based on the diff.
	var (
		migrationType  db.ReindexMigrationType
		properties     []string
		targetTok      string
		bucketStrategy string
	)

	switch {
	// enable-searchable must be matched BEFORE change-tokenization: an
	// enable request carries tokenization in the same body, but a property
	// that has no searchable index yet cannot have its tokenization
	// "changed" — validateTokenizationChange would fail looking for a
	// non-existent searchable bucket.
	case body.Searchable != nil && body.Searchable.Enabled:
		migrationType = db.ReindexTypeEnableSearchable
		properties = []string{propertyName}
		targetTok = body.Searchable.Tokenization
		if err := validateEnableSearchableProperty(targetProp, targetTok); err != nil {
			return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(principal, err.Error()))
		}

	case body.Searchable != nil && body.Searchable.Tokenization != "":
		// Change tokenization on a property whose searchable index already
		// exists. If Enabled was also set it would have matched the case
		// above.
		migrationType = db.ReindexTypeChangeTokenization
		properties = []string{propertyName}
		targetTok = body.Searchable.Tokenization

		// Reject early when the property has no searchable index. Otherwise
		// the downstream validator surfaces a "searchable bucket not
		// found" error that doesn't tell the caller what to do — they
		// just see a 400 and the dialog hangs. Filterable-only properties
		// should use {filterable: {tokenization: X}} instead.
		if targetProp.IndexSearchable != nil && !*targetProp.IndexSearchable {
			return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(principal,
				db.NoSearchableIndexError(propertyName, db.NoSearchableIndexHintTokenization)))
		}

		var err error
		bucketStrategy, err = validateTokenizationChange(h.appState, class, collection, propertyName, targetTok)
		if err != nil {
			return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(principal, err.Error()))
		}

	case body.Filterable != nil && body.Filterable.Tokenization != "":
		// Change tokenization on a property whose filterable index exists.
		// Differs from {searchable:{tokenization:X}}: this variant
		// retokenizes ONLY the filterable bucket, never the searchable.
		// The right shape for filterable-only text/text[] properties, and
		// also valid when the property has both indexes and the caller
		// wants to retokenize only the filterable side (rare but
		// well-defined: filterable uses Equal semantics, retokenizing it
		// independently of searchable is meaningful).
		migrationType = db.ReindexTypeChangeTokenizationFilterable
		properties = []string{propertyName}
		targetTok = body.Filterable.Tokenization

		if err := validateFilterableTokenizationChange(targetProp, targetTok); err != nil {
			return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(principal, err.Error()))
		}

	case body.Searchable != nil && body.Searchable.Rebuild:
		if targetProp.IndexSearchable != nil && !*targetProp.IndexSearchable {
			return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(principal,
				db.NoSearchableIndexError(propertyName, db.NoSearchableIndexHintRebuildOrAlgorithm)))
		}
		// rebuild preserves the current BM25 algorithm and tokenization.
		// WAND searchable indexes cannot be rebuilt — the only supported
		// next step for them is migration to BlockMax via
		// {"searchable":{"algorithm":"blockmax"}}.
		if class.InvertedIndexConfig == nil || !class.InvertedIndexConfig.UsingBlockMaxWAND {
			return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(principal,
				"cannot rebuild a WAND searchable index — WAND is deprecated; use {\"searchable\":{\"algorithm\":\"blockmax\"}} to migrate first"))
		}
		migrationType = db.ReindexTypeRebuildSearchable
		properties = []string{propertyName}

	case body.Searchable != nil && body.Searchable.Algorithm != "":
		if targetProp.IndexSearchable != nil && !*targetProp.IndexSearchable {
			return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(principal,
				db.NoSearchableIndexError(propertyName, db.NoSearchableIndexHintRebuildOrAlgorithm)))
		}
		// Canonicalise the algorithm name through normalizeSearchableAlgorithm,
		// then dispatch on the canonical value with an explicit allowlist.
		//
		// The explicit `switch` is deliberately stricter than an equality
		// check: when a second searchable algorithm eventually ships, the
		// swagger enum will accept it and unrelated handler call sites will
		// silently start receiving the new value here. With an inline
		// `if x != "blockmax"` the new algorithm would either be silently
		// rejected (bad UX) or silently accepted with no migration type
		// wired up (data corruption). The `switch` instead surfaces every
		// added algorithm as a missing case the compiler / reviewers can
		// see at the diff site. WAND is explicitly listed as the deprecated
		// arm so the error message stays accurate when it lands as input.
		normalized := normalizeSearchableAlgorithm(body.Searchable.Algorithm)
		switch normalized {
		case models.IndexStatusAlgorithmBlockmax:
			// supported target — fall through to submit
		case models.IndexStatusAlgorithmWand:
			return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(principal,
				fmt.Sprintf("algorithm %q is deprecated; only %q is accepted as a target",
					models.IndexStatusAlgorithmWand, models.IndexStatusAlgorithmBlockmax)))
		default:
			return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(principal,
				fmt.Sprintf("unsupported algorithm %q; only %q is accepted (WAND is deprecated)",
					body.Searchable.Algorithm, models.IndexStatusAlgorithmBlockmax)))
		}
		if class.InvertedIndexConfig != nil && class.InvertedIndexConfig.UsingBlockMaxWAND {
			return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(principal,
				"searchable index is already on blockmax"))
		}
		migrationType = db.ReindexTypeChangeAlgorithm
		properties = []string{propertyName}

	case body.Filterable != nil && body.Filterable.Enabled:
		migrationType = db.ReindexTypeEnableFilterable
		properties = []string{propertyName}
		if err := validateEnableFilterableProperty(targetProp); err != nil {
			return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(principal, err.Error()))
		}

	case body.Filterable != nil && body.Filterable.Rebuild:
		migrationType = db.ReindexTypeRepairFilterable
		properties = []string{propertyName}
		if targetProp.IndexFilterable != nil && !*targetProp.IndexFilterable {
			return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(principal,
				fmt.Sprintf("property %q does not have a filterable index", propertyName)))
		}
		if err := validateRebuildFilterableDataType(targetProp); err != nil {
			return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(principal, err.Error()))
		}

	case body.Rangeable != nil && body.Rangeable.Enabled:
		migrationType = db.ReindexTypeEnableRangeable
		properties = []string{propertyName}
		if err := validateRangeableProperties(class, properties); err != nil {
			return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(principal, err.Error()))
		}

	case body.Rangeable != nil && body.Rangeable.Rebuild:
		migrationType = db.ReindexTypeRepairRangeable
		properties = []string{propertyName}
		if err := validateRebuildRangeableProperty(targetProp); err != nil {
			return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(principal, err.Error()))
		}

	default:
		// The verb list must enumerate EVERY dispatch case above. A missing
		// verb here ships as a confusing 400 ("you sent a valid body shape
		// but the error says it's invalid") and was the symptom flagged on
		// weaviate/0-weaviate-issues#227 (Gap 7). Order: per index-group,
		// then alphabetical within group.
		return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(principal,
			"no actionable change detected; set one of: "+
				"searchable.algorithm, searchable.cancel, searchable.enabled, searchable.rebuild, searchable.tokenization, "+
				"filterable.cancel, filterable.enabled, filterable.rebuild, filterable.tokenization, "+
				"rangeable.cancel, rangeable.enabled, rangeable.rebuild"))
	}

	// --- Multi-tenancy handling ---
	isMT := class.MultiTenancyConfig != nil && class.MultiTenancyConfig.Enabled
	tenants := params.Tenants
	semantic := db.IsSemanticMigration(migrationType)

	// Validate MT + tenants combination.
	if !isMT && len(tenants) > 0 {
		return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(
			errorResponse(principal, "tenants parameter is only valid for multi-tenant collections"))
	}
	if semantic && len(tenants) > 0 {
		return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(
			errorResponse(principal, "tenants parameter cannot be used with semantic migrations (change-tokenization); all tenants must be targeted"))
	}

	// For MT collections with specific tenants, validate they exist and are not OFFLOADED/FROZEN.
	if isMT && len(tenants) > 0 {
		if err := validateTenants(h.appState.DB, params.HTTPRequest.Context(), collection, tenants); err != nil {
			return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(principal, err.Error()))
		}
	}

	// Build unit maps from shard placement. Use ShardReplicaOwnership (not
	// ShardOwnership) to create one unit per shard per replica node. Each
	// replica has its own local copy of the data that must be reindexed.
	ctx := params.HTTPRequest.Context()
	var shardOwnership map[string][]string
	var err error
	if isMT {
		shardOwnership, err = h.appState.DB.ShardReplicaOwnershipForMT(ctx, collection, tenants)
	} else {
		shardOwnership, err = h.appState.DB.ShardReplicaOwnership(ctx, collection)
	}
	if err != nil {
		return schema.NewSchemaObjectsIndexesUpdateInternalServerError().WithPayload(
			errorResponse(principal, fmt.Sprintf("getting shard ownership: %v", err)))
	}
	if len(shardOwnership) == 0 {
		return schema.NewSchemaObjectsIndexesUpdateBadRequest().WithPayload(errorResponse(principal, "collection has no shards"))
	}

	unitIDs, unitToShard, unitToNode := buildUnitMaps(shardOwnership)

	// Capture the property's tokenization at submit-time. OnTaskCompleted
	// will check this in the schema-flip mutator so a post-restart
	// FSM-replay of an older task can't override a newer task's already-
	// applied schema flip. See the OriginalTokenization godoc on
	// ReindexTaskPayload for the full rationale.
	var originalTok string
	if migrationType == db.ReindexTypeChangeTokenization ||
		migrationType == db.ReindexTypeChangeTokenizationFilterable ||
		migrationType == db.ReindexTypeEnableSearchable {
		originalTok = targetProp.Tokenization
	}

	payload := db.ReindexTaskPayload{
		MigrationType:        migrationType,
		Collection:           collection,
		Properties:           properties,
		TargetTokenization:   targetTok,
		OriginalTokenization: originalTok,
		BucketStrategy:       bucketStrategy,
		Tenants:              tenants,
		UnitToNode:           unitToNode,
		UnitToShard:          unitToShard,
	}

	// Build a human-readable task ID with a random suffix for uniqueness.
	// Format: "Collection:migration-type:property:ab3f" (or without property for whole-collection ops).
	suffix := shortRandomSuffix()
	taskID := fmt.Sprintf("%s:%s:%s", collection, migrationType, suffix)
	if len(properties) > 0 {
		taskID = fmt.Sprintf("%s:%s:%s:%s", collection, migrationType, properties[0], suffix)
	}

	// Note: propLock for (collection, propertyName) was acquired at
	// the top of this handler — before the class read and validation —
	// so the conflict-check + AddDistributedTask + DELETE-property-
	// index races are all serialized through the same lock entry. See
	// the early-acquisition comment up top + [state.ReindexSubmitLocks]
	// godoc for the multi-node caveat.

	// Check for conflicting active tasks. Any two reindex migrations on
	// the same (collection, property) tuple conflict; see typesConflict's
	// godoc for the on-disk state race that motivated the rule.
	if h.tasks != nil {
		tasks, err := h.tasks.ListDistributedTasks(ctx)
		if err == nil {
			reason, checkErr := checkReindexConflict(collection, migrationType, properties, tasks[db.ReindexNamespace])
			if checkErr != nil {
				// An in-flight task has an unparseable payload — we cannot
				// prove the new submit doesn't conflict with it, so refuse
				// rather than race. Return 503 so the caller knows to retry
				// after an operator inspects the in-flight task.
				return schema.NewSchemaObjectsIndexesUpdateServiceUnavailable().WithPayload(errorResponse(principal, checkErr.Error()))
			}
			if reason != "" {
				return schema.NewSchemaObjectsIndexesUpdateConflict().WithPayload(errorResponse(principal, reason))
			}
			// Per-collection cap on concurrent STARTED reindex tasks. Without
			// this a caller scripting `for p in $(properties); do PUT
			// .../indexes/$p; done` against an N-property collection submits N
			// independent RAFT tasks, each fanning out ingest+backup buckets
			// on every replica. The LSM compaction layer and disk would not
			// survive that. Reject with 429 once the cap is reached — the
			// semantics ("retry later, you're over a concurrency limit") map
			// exactly to RFC 6585's Too Many Requests, not to 503's "server
			// is unavailable". Returning 503 here misled callers and
			// monitoring into thinking the cluster was unhealthy rather than
			// rate-limiting them.
			if inflight := countStartedTasksForCollection(collection, tasks[db.ReindexNamespace]); inflight >= maxConcurrentReindexPerCollection {
				return reindexCapExceededResponder(principal, collection, inflight, maxConcurrentReindexPerCollection)
			}
		}
	}

	// Refuse from this node's own slots before the gate below is taken. The gate
	// closes the backup gate on the whole collection, so a submission that is
	// certain to be refused anyway would otherwise fail a capture that is already
	// running — the lowest-priority operation killing a higher-priority one while
	// being denied. Reading the local slots is an in-memory map lookup, so it
	// costs nothing to do first.
	//
	// This does not replace the probe below: a backup held only by another node
	// is invisible here, still takes the gate, and is still refused by the
	// fan-out. It removes the single-node case, which is the one a retrying
	// caller can loop on.
	if responder := h.refuseOnLocalBackupActivity(principal); responder != nil {
		return responder
	}

	// Taken BEFORE the probe, not after it. The probe fans out over every node
	// in the cluster, so a backup can claim its slot on a node that was already
	// answered while the scan is still running: it sees no submission, gets
	// admitted, and then has its sidecar dirs and .migrations tracker removed
	// underneath it by the sweep below. Closing the gate first makes the probe
	// and everything after it one window, so whichever side closes first is the
	// one the other sees.
	//
	// The post-commit rollback cannot repair this. It manufactures a cancelled
	// task no unit was ever claimed on, which the commit-time backstop waives
	// on purpose — see [db.ReindexProvider] and reindexTaskOverlaps.
	//
	// Set when the gate is taken; the rollback path releases it early, so it
	// cannot be a plain deferred call.
	releaseSubmitGate := func() {}
	defer func() { releaseSubmitGate() }()

	indexTypesForCleanup, indexTypeKnown := indexTypesFromMigrationType(migrationType)
	if indexTypeKnown {
		if provider := h.appState.ReindexProvider.Load(); provider != nil {
			releaseSubmitGate = sync.OnceFunc(provider.MarkSubmitInProgress(collection))
		}
	}

	// Runs after the free local checks; this one costs a cluster-wide round trip.
	if _, responder := h.probeBackupActivity(ctx, principal); responder != nil {
		return responder
	}

	// Defense in depth against the CANCEL→retry silent failure (same Sev 1
	// family as DELETE→re-enable, fixed in 6b7dc23768): if a previous
	// cancelled run left stale .migrations/<dir>/started.mig +
	// __reindex/__ingest sidecars on disk, the new task would resume
	// against them — finish in <1s with a 50-entry no-op — flip the
	// schema flag, and report success against an empty bucket.
	//
	// The cancel handler already runs this cleanup synchronously, but
	// only after waiting for the local goroutine to drain. The wait can
	// time out (or be skipped entirely if the node crashed mid-cancel),
	// in which case the on-disk state survives. Running it again here,
	// AFTER checkReindexConflict has confirmed no STARTED task targets
	// this (collection, prop, index) tuple, closes that gap.
	//
	// Safe to call even when no stale state exists: missing buckets and
	// missing directories are silently skipped by the per-shard helper.
	//
	// The gate covering this deletion was taken above the probe; it is held
	// until the handler returns so the deletion and the commit are one window.
	if indexTypeKnown {
		// Loop over every index type this migration touches. For
		// single-index migrations the slice has one entry; for
		// change-tokenization-both (which writes searchable AND filterable
		// sub-task dirs) it has two. Cleaning BOTH is critical — see the
		// indexTypesFromMigrationType godoc for the Sev 1 data-loss bug
		// that motivated the multi-index sweep.
		//
		// Detached from the request like the cancel handler's sweep: this one is
		// the retry for state an earlier run left behind, so failing it for a
		// reason we control just defers the work again. It only matters while
		// sidecar buckets are still loaded (the shape a crash leaves), which is
		// the sole point the sweep consults its context — see
		// [Shard.CleanStalePartialReindexState].
		cleanupCtx, cancelCleanup := context.WithTimeout(
			context.WithoutCancel(ctx), reindexCancelCleanupTimeout)
		defer cancelCleanup()
		for _, indexTypeForCleanup := range indexTypesForCleanup {
			if err := h.appState.DB.CleanStalePartialReindexState(cleanupCtx, collection, propertyName, indexTypeForCleanup); err != nil {
				h.appState.Logger.WithFields(logrus.Fields{
					"collection":     collection,
					"property":       propertyName,
					"migration_type": migrationType,
					"index_type":     indexTypeForCleanup,
				}).Errorf("submit: pre-submit cleanup of stale partial reindex state failed: %v; the new task may short-circuit on the stale state and report a false success — operator inspection recommended", err)
			}
		}
	}

	// Unlike the conflict checks above, submitting requires the cluster service.
	if h.tasks == nil {
		return schema.NewSchemaObjectsIndexesUpdateServiceUnavailable().WithPayload(errorResponse(principal,
			"cluster service unavailable; cannot submit reindex task"))
	}

	// Semantic migrations opt into the two-phase RAFT PREP barrier;
	// MT semantic migrations also group by tenant for per-tenant barriers.
	if isMT && semantic {
		unitSpecs := buildUnitSpecs(shardOwnership)
		if err := h.tasks.AddDistributedTaskWithGroupsBarrier(
			ctx, db.ReindexNamespace, taskID, payload, unitSpecs, semantic,
		); err != nil {
			return schema.NewSchemaObjectsIndexesUpdateInternalServerError().WithPayload(
				errorResponse(principal, fmt.Sprintf("submitting task: %v", err)))
		}
	} else {
		if err := h.tasks.AddDistributedTaskWithBarrier(
			ctx, db.ReindexNamespace, taskID, payload, unitIDs, semantic,
		); err != nil {
			return schema.NewSchemaObjectsIndexesUpdateInternalServerError().WithPayload(
				errorResponse(principal, fmt.Sprintf("submitting task: %v", err)))
		}
	}

	// Second probe, now that the task is committed: a backup that claimed its
	// slot before this point saw nothing to refuse. From here on, whichever
	// side committed second sees the other. We roll back; the backup can't.
	scan, responder := h.probeBackupActivity(ctx, principal)
	if responder != nil {
		// Only a node that positively reports a backup is evidence one claimed
		// the slot. "Nobody answered" is not: a client disconnect cancels this
		// context, which makes every probe fail, and rolling back on that would
		// destroy a cleanly committed migration precisely when the caller is no
		// longer there to resubmit it.
		//
		// A positive report is not weakened by the same disconnect: the node
		// answered before the context died, and the migration would otherwise
		// run against a backup that is known to hold the slot. That is why
		// rollbackRacedReindexTask detaches from this context.
		if scan.BusyNode != "" {
			// Hand back the submit lock and gate (in acquisition-inverse order)
			// before rolling back. Both exist to keep the schema read, the
			// sidecar sweep and the task-add one window, and the task is now
			// committed. Holding them across a rollback of up to
			// reindexRollbackTimeout would make a disconnected client — whose
			// request nobody is waiting for — block an unrelated DELETE on this
			// property and every backup of this collection for those 10s.
			//
			// The rollback stays on this goroutine: the caller, if still there,
			// must not be told the submission was refused before the rollback
			// was attempted.
			releaseSubmitGate()
			releaseSubmitLock()
			h.rollbackRacedReindexTask(ctx, taskID, collection, propertyName)
			return responder
		}
		h.appState.Logger.WithFields(logrus.Fields{
			"audit_event": "reindex_task_kept_after_unconfirmed_probe",
			"taskID":      taskID,
			"collection":  collection,
			"property":    propertyName,
		}).Error("submit: the post-commit probe could not confirm the cluster is free of backups, " +
			"so the task was left running rather than rolled back on unreliable evidence; " +
			"the backup side's commit-time overlap check is the remaining guard")
		// The migration is committed and running. Answering without its id sends
		// the caller into a retry that checkReindexConflict answers 409, for a
		// task the caller was never told about.
		return reindexTaskKeptResponder(principal, namespacing.StripOwnNamespace(principal, taskID))
	}

	// Operational audit line: reindex is a privileged cluster-wide operation
	// (rebuilds buckets on every replica, flips schema flags). Log the who,
	// what, and which task ID at submit time so ops can grep for it later.
	// RBAC audit logging upstream covers the authorize/deny decision; this
	// log covers the successful submission.
	h.appState.Logger.WithFields(logrus.Fields{
		"audit_event":    "reindex_task_submitted",
		"taskID":         taskID,
		"collection":     collection,
		"property":       propertyName,
		"migration_type": migrationType,
		"principal":      principalUsername(principal),
	}).Info("reindex provider: submitted task")

	return schema.NewSchemaObjectsIndexesUpdateAccepted().WithPayload(&models.IndexUpdateResponse{
		// The task ID embeds the qualified collection.
		TaskID: namespacing.StripOwnNamespace(principal, taskID),
		Status: "STARTED",
	})
}

// reindexOwnerGateTimeout bounds the wait for ONE remote owner to close its
// cleanup gate. A cancel must never become unanswerable because a node is
// unreachable, so this is short and the handler proceeds either way. Owners are
// probed concurrently, so it also bounds the whole wait.
//
// The owners close their gates as the cancel applies rather than when the
// scheduler next ticks, which is what makes this budget meetable at all; see
// [db.ReindexProvider.OnTerminalApplied].
const reindexOwnerGateTimeout = 5 * time.Second

// reindexOwnerGatePollInterval is how often each owner is re-asked.
const reindexOwnerGatePollInterval = 100 * time.Millisecond

// awaitOwnerCleanupGates blocks until every other node owning a unit of the
// cancelled task reports its reindex-cleanup gate closed, or until the bound
// elapses.
//
// The node handling a cancel may own none of the collection's shards, so it has
// nothing of its own to tear down and would answer while the owners are still a
// DTM hook away from closing theirs — a window in which a backup can start into
// a teardown the caller was told had been ordered.
//
// It never fails the cancel: the task is already cancelled, and a caller who
// cannot cancel at all is worse off than one told about a smaller window.
func (h *indexesHandlers) awaitOwnerCleanupGates(ctx context.Context, payload *db.ReindexTaskPayload, collection, taskID string) {
	if h.reindexCleanup == nil {
		return
	}
	local := ""
	if h.cluster != nil {
		local = h.cluster.LocalName()
	}
	owners := make([]string, 0, len(payload.UnitToNode))
	seen := make(map[string]struct{}, len(payload.UnitToNode))
	for _, node := range payload.UnitToNode {
		if node == "" || node == local {
			continue
		}
		if _, ok := seen[node]; ok {
			continue
		}
		seen[node] = struct{}{}
		owners = append(owners, node)
	}
	if len(owners) == 0 {
		return
	}

	// One budget per owner, spent concurrently. Sharing a single deadline
	// across a sequential loop let the first slow owner spend it, leaving the
	// rest one probe each against an already-expired context.
	reasons := make([]string, len(owners))
	var wg sync.WaitGroup
	wg.Add(len(owners))
	for i, node := range owners {
		enterrors.GoWrapper(func() {
			defer wg.Done()
			reasons[i] = h.awaitOneOwnerCleanupGate(ctx, node, collection)
		}, h.appState.Logger)
	}
	wg.Wait()

	degraded := map[string]string{}
	for i, reason := range reasons {
		if reason != "" {
			degraded[owners[i]] = reason
		}
	}

	if len(degraded) > 0 {
		h.appState.Logger.WithFields(logrus.Fields{
			"audit_event": "reindex_cancel_gate_unconfirmed",
			"taskID":      taskID,
			"collection":  collection,
			"nodes":       degraded,
		}).Warn("cancel: could not confirm every owner closed its cleanup gate; " +
			"answering anyway — a backup started right now could still catch the teardown on those nodes")
	}
}

// awaitOneOwnerCleanupGate polls one owner until it reports its cleanup gate
// closed, returning "" on confirmation or the reason it could not be confirmed.
func (h *indexesHandlers) awaitOneOwnerCleanupGate(ctx context.Context, node, collection string) string {
	waitCtx, cancel := context.WithTimeout(ctx, reindexOwnerGateTimeout)
	defer cancel()

	for {
		closed, err := h.reindexCleanup.CleanupInProgress(waitCtx, node, collection)
		if errors.Is(err, clients.ErrReindexCleanupUnsupported) {
			// An older build cannot answer; waiting would burn the whole
			// budget to learn nothing.
			return "node does not serve the cleanup probe"
		}
		if err == nil && closed {
			return ""
		}
		select {
		case <-waitCtx.Done():
			if err != nil {
				return err.Error()
			}
			return "gate not closed within " + reindexOwnerGateTimeout.String()
		case <-time.After(reindexOwnerGatePollInterval):
		}
	}
}

// reindexRollbackTimeout bounds the rollback, which the refused PUT waits on.
// A rollback that cannot finish quickly is better abandoned to the backup's
// commit-time check than left holding the request open.
const reindexRollbackTimeout = 10 * time.Second

// reindexRollbackAttempts bounds the rollback retry. A cancel that fails three
// times in a row is failing for a reason a fourth will not fix.
const reindexRollbackAttempts = 3

// reindexRollbackRetryDelay is the first wait between rollback attempts; it
// grows and is jittered from there. The transient this retry exists for is a
// RAFT leader election, which lasts seconds while failing in microseconds, so
// back-to-back attempts would spend all three inside a millisecond and never
// outlive the condition they are retrying.
const reindexRollbackRetryDelay = 500 * time.Millisecond

// rollbackRacedReindexTask cancels a task committed into a backup that
// claimed the same slot.
//
// It runs before the 409 is answered: a rollback that never lands leaves a
// migration running that its submitter believes was refused. A final failure is
// logged at Error under its own audit event so the state is findable; leaving
// the task is safer than a blind second cancel, and the backup side's
// commit-time overlap check still refuses to publish a backup that spans it.
//
// It deliberately does not run on the request context. A client disconnect is
// itself one of the inputs that makes the second probe report every node
// unreachable, so the very condition that decides a rollback is needed would
// also kill it, leaving the task STARTED against a live backup.
//
// Each rollback leaves a CANCELLED task in DTM until the retention window drops
// it, and those are not inert: the commit-time overlap check reads them and
// classifies them by unit state — see reindexTaskTouchedShards, whose waiver
// exists precisely so a rollback that never claimed a unit stays harmless.
// Bounding the accumulation would mean deleting records that backstop needs;
// the retention window is the intended bound and is operator-tunable.
func (h *indexesHandlers) rollbackRacedReindexTask(ctx context.Context, taskID, collection, propertyName string) {
	fields := logrus.Fields{
		"audit_event": "reindex_task_rolled_back",
		"taskID":      taskID,
		"collection":  collection,
		"property":    propertyName,
	}

	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), reindexRollbackTimeout)
	defer cancel()

	delays := newRollbackRetryBackoff(reindexRollbackRetryDelay, reindexRollbackTimeout)

	var lastErr error
	for attempt := 1; attempt <= reindexRollbackAttempts; attempt++ {
		done, err := h.tryRollbackRacedReindexTask(ctx, taskID, fields)
		if done {
			return
		}
		lastErr = err
		if ctx.Err() != nil {
			break
		}
		if attempt < reindexRollbackAttempts && !waitBeforeRollbackRetry(ctx, delays) {
			break
		}
	}
	h.appState.Logger.WithFields(fields).WithField("audit_event", "reindex_task_rollback_failed").Errorf(
		"rollback: could not cancel the task in %d attempts: %v; it is still running while its submitter was told the "+
			"submission was refused — cancel it by hand", reindexRollbackAttempts, lastErr)
}

// newRollbackRetryBackoff builds the rollback retry schedule.
//
// The options have to be passed to the constructor rather than assigned to the
// returned struct: the constructor snapshots the initial interval into the
// interval it will actually hand out, and a field assigned afterwards is only
// picked up by a later Reset. This schedule is stepped directly by
// waitBeforeRollbackRetry, so nothing calls Reset and a field assigned
// afterwards would never be read.
func newRollbackRetryBackoff(initialInterval, maxElapsedTime time.Duration) backoff.BackOff {
	return backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(initialInterval),
		backoff.WithMaxElapsedTime(maxElapsedTime),
	)
}

// waitBeforeRollbackRetry waits out the next backoff step, reporting whether
// another attempt is still worth making.
func waitBeforeRollbackRetry(ctx context.Context, delays backoff.BackOff) bool {
	delay := delays.NextBackOff()
	if delay == backoff.Stop {
		return false
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// tryRollbackRacedReindexTask makes one rollback attempt. done is true when
// there is nothing left to do, whether it cancelled the task or found it gone.
func (h *indexesHandlers) tryRollbackRacedReindexTask(
	ctx context.Context, taskID string, fields logrus.Fields,
) (bool, error) {
	tasks, err := h.tasks.ListDistributedTasks(ctx)
	if err != nil {
		return false, fmt.Errorf("listing tasks: %w", err)
	}
	for _, task := range tasks[db.ReindexNamespace] {
		if task.ID != taskID {
			continue
		}
		if task.Status.IsTerminal() {
			// The rollback wants the task not running, and it already is not.
			// Read from the observed status rather than from the cancel's
			// error: the FSM answers a PREPARING or SWAPPING task, and a
			// version that moved under a STARTED one, with the same permanent
			// rejection, and those tasks are still live. Treating the error as
			// proof of a terminal status would declare a running migration
			// settled and skip the Error line telling the operator to cancel it
			// by hand. A task that goes terminal between this listing and the
			// cancel below is caught by the next attempt's listing.
			h.appState.Logger.WithFields(fields).
				WithField("audit_event", "reindex_task_rollback_already_terminal").
				WithField("task_status", task.Status.String()).
				Info("rollback: the reindex task that raced a backup claim had already reached a terminal status")
			return true, nil
		}
		if err := h.tasks.CancelDistributedTask(ctx, task.Namespace, task.ID, task.Version); err != nil {
			return false, fmt.Errorf("cancelling: %w", err)
		}
		h.appState.Logger.WithFields(fields).Info("rollback: cancelled a reindex task that raced a backup claim")
		return true, nil
	}
	// Nothing was rolled back here: the task is absent from the listing, so
	// there is nothing left to cancel. The audit label says so rather than
	// claiming a rollback that did not happen.
	h.appState.Logger.WithFields(fields).WithField("audit_event", "reindex_task_rollback_not_needed").
		Warn("rollback: the task was already gone")
	return true, nil
}

// backupActivityScanTimeout bounds the cluster fan-out so one hung node cannot hang the PUT.
const backupActivityScanTimeout = 5 * time.Second

type nodeActivityProber interface {
	NodeActivity(ctx context.Context, nodeName string) (backup.NodeActivity, error)
}

// backupActivityScan is the verdict of probing every node for a backup or restore slot.
type backupActivityScan struct {
	BusyNode string
	Activity backup.NodeActivity

	UnreachableNode string
	UnreachableErr  error
}

// scanBackupActivity probes every node in parallel; results are indexed by
// position so the reported node is deterministic regardless of answer order.
func scanBackupActivity(ctx context.Context, nodes []string, prober nodeActivityProber, logger logrus.FieldLogger) backupActivityScan {
	ctx, cancel := context.WithTimeout(ctx, backupActivityScanTimeout)
	defer cancel()

	type result struct {
		activity backup.NodeActivity
		err      error
	}
	results := make([]result, len(nodes))
	// Seed every slot with a failure so an unwritten one cannot read as a clear
	// node. GoWrapper recovers a panicking prober, and the deferred wg.Done runs
	// during that unwinding, so wg.Wait returns normally over a slot that was
	// never assigned. Its zero value is {Busy: false, err: nil}, which matches
	// none of the classifier arms below and falls through as "this node has no
	// backup running". Overwritten by every probe that does report.
	for i := range results {
		results[i].err = errors.New("probe did not report")
	}

	var wg sync.WaitGroup
	wg.Add(len(nodes))
	for i, node := range nodes {
		enterrors.GoWrapper(func() {
			defer wg.Done()
			activity, err := prober.NodeActivity(ctx, node)
			results[i] = result{activity: activity, err: err}
		}, logger)
	}
	wg.Wait()

	var scan backupActivityScan
	for i, res := range results {
		switch {
		case errors.Is(res.err, clients.ErrNodeActivityUnsupported):
			logger.WithField("action", "reindex_backup_gate").WithField("node", nodes[i]).
				Warn("node does not serve the backup activity probe; treating it as free of backups. " +
					"Expected while a rolling upgrade is in progress.")
		case res.err != nil:
			if scan.UnreachableNode == "" {
				scan.UnreachableNode = nodes[i]
				scan.UnreachableErr = res.err
			}
		case res.activity.Busy:
			if scan.BusyNode == "" {
				scan.BusyNode = nodes[i]
				scan.Activity = res.activity
			}
		}
	}
	return scan
}

// backupActivityResponder turns a scan into the refusal it warrants, or nil if clear.
// Node names, backup IDs and transport errors stay out of the body: they need
// read_nodes/read_backups, but this handler only requires update_collections. See the node log for detail.
func backupActivityResponder(principal *models.Principal, scan backupActivityScan) middleware.Responder {
	// A definite "busy" outranks an unreachable node: it's a certain answer.
	if scan.BusyNode != "" {
		return backupBusyResponder(principal, scan.Activity)
	}
	if scan.UnreachableNode != "" {
		return schema.NewSchemaObjectsIndexesUpdateServiceUnavailable().WithPayload(errorResponse(principal,
			"reindex blocked: cannot confirm the cluster is free of backups; retry once every node answers"))
	}
	return nil
}

// reindexTaskKeptResponder is the refusal for a migration that is committed and
// running while the post-commit probe could not confirm the cluster is free of
// backups. Same 503 as [backupActivityResponder]'s unreachable verdict, plus the
// task id: the caller is being refused for a migration that did start, and the
// id is the only handle it has on it.
func reindexTaskKeptResponder(principal *models.Principal, taskID string) middleware.Responder {
	return schema.NewSchemaObjectsIndexesUpdateServiceUnavailable().WithPayload(errorResponse(principal,
		fmt.Sprintf("reindex blocked: cannot confirm the cluster is free of backups; retry once every node answers. "+
			"The migration was committed and is running as task %q; cancel it if you do not want it to continue.",
			taskID)))
}

// backupBusyResponder is the refusal for a node that certainly holds a slot,
// whichever check found it. The kind is all that reaches the caller; see
// [backupActivityResponder] for why the node and backup id do not.
func backupBusyResponder(principal *models.Principal, activity backup.NodeActivity) middleware.Responder {
	return schema.NewSchemaObjectsIndexesUpdateConflict().WithPayload(errorResponse(principal,
		fmt.Sprintf("reindex blocked: a %s is running in the cluster; retry after it finishes",
			activity.Kind)))
}

// refuseOnLocalBackupActivity answers with the fan-out probe's refusal when
// this node's own slots already settle the question. Only this node is visible
// to it, so it is a pre-check and not a replacement: it exists so the caller is
// turned away before the submit gate closes the backup gate on the collection.
func (h *indexesHandlers) refuseOnLocalBackupActivity(principal *models.Principal) middleware.Responder {
	if h.localBackupActivity == nil {
		return nil
	}
	activity := h.localBackupActivity.Activity()
	if !activity.Busy {
		return nil
	}

	h.appState.Logger.WithField("action", "reindex_backup_gate").
		WithField("backup_id", activity.ID).
		Infof("refusing reindex submission before the submit gate: this node is running a %s", activity.Kind)

	return backupBusyResponder(principal, activity)
}

// probeBackupActivity blocks reindex submission while any node holds a backup
// or restore slot, mirroring backups refusing to start under a running reindex.
// It returns the scan as well as the refusal, for the post-commit caller that
// must tell a definite "busy" apart from "nobody answered".
func (h *indexesHandlers) probeBackupActivity(ctx context.Context, principal *models.Principal) (backupActivityScan, middleware.Responder) {
	var nodes []string
	if h.cluster != nil {
		nodes = h.cluster.AllNames()
	}

	if h.backupActivity == nil || len(nodes) == 0 {
		h.backupActivityGateWarn().WithSampling(func(logrus.FieldLogger) {
			h.appState.Logger.WithField("action", "reindex_backup_gate").
				Warn("backup activity probe is not wired; allowing reindex submission without checking for running backups. " +
					"Expected in test fixtures; if this appears in production, check the BackupActivity wiring in configure_api.go.")
		})
		return backupActivityScan{}, nil
	}

	// A node that left the cluster isn't probed; its slots died with its process.
	scan := scanBackupActivity(ctx, nodes, h.backupActivity, h.appState.Logger)

	// Detail withheld from the response body (see backupActivityResponder) goes here.
	entry := h.appState.Logger.WithField("action", "reindex_backup_gate")
	switch {
	case scan.BusyNode != "":
		entry.WithField("node", scan.BusyNode).WithField("backup_id", scan.Activity.ID).
			Infof("refusing reindex submission: node is running a %s", scan.Activity.Kind)
	case scan.UnreachableNode != "":
		entry.WithField("node", scan.UnreachableNode).
			Warnf("refusing reindex submission: node did not answer the backup activity probe: %v", scan.UnreachableErr)
	}

	return scan, backupActivityResponder(principal, scan)
}

// principalUsername extracts the user-facing identifier from a principal
// for audit logging. Falls back to "anonymous" if the principal is nil.
func principalUsername(principal *models.Principal) string {
	if principal == nil {
		return "anonymous"
	}
	return principal.Username
}

// requestedCancel returns (indexType, true) if the body asks to cancel an
// in-flight reindex on this property, where indexType is one of
// "filterable", "searchable", or "rangeable". Returns ("", false)
// otherwise. validateBodyExclusivity has already guaranteed at most one
// cancel field is set across the body.
// requestsCancel is the nil-safe form of [requestedCancel], used by the
// RUNTIME_REINDEX_ENABLED check before the body has been validated.
func requestsCancel(body *models.IndexUpdateRequest) bool {
	if body == nil {
		return false
	}
	_, cancelling := requestedCancel(body)
	return cancelling
}

func requestedCancel(body *models.IndexUpdateRequest) (string, bool) {
	switch {
	case body.Searchable != nil && body.Searchable.Cancel:
		return "searchable", true
	case body.Filterable != nil && body.Filterable.Cancel:
		return "filterable", true
	case body.Rangeable != nil && body.Rangeable.Cancel:
		return "rangeable", true
	}
	return "", false
}

// cancelReindexTask finds the STARTED reindex task targeting
// (collection, propertyName, indexType) and asks DTM to cancel it.
//
// Idempotent cancel: by the time this runs the caller's (collection,
// property) tuple has already been verified to exist by [updateIndex] —
// a missing class or property would have produced a 404 there. So when
// no STARTED task matches the cancel target we return 202 + Status:
// NO_OP rather than 404. That mirrors how callers think about cancel:
// "make sure no reindex is running on this property" is the same
// idempotent intent whether or not a task happened to be in flight at
// request time. The previous 404 conflated "the cancel target is
// unknown" with "there is nothing to cancel" — callers couldn't
// disambiguate without parsing the response body, and scripts that
// expect "this task is cancelled now" had to special-case 404 as a
// success.
//
// On success: 202 + Status: CANCELLED with the cancelled task's ID. The
// DTM scheduler picks up the CANCELLED state on its next tick and
// terminates the local handle; the task's ctx (the provider's per-task
// ctx via runningHandles) is then cancelled, and the worker goroutine
// returns.
func (h *indexesHandlers) cancelReindexTask(ctx context.Context, collection, propertyName, indexType string, principal *models.Principal) middleware.Responder {
	if h.tasks == nil {
		return schema.NewSchemaObjectsIndexesUpdateServiceUnavailable().WithPayload(errorResponse(principal,
			"cluster service unavailable; cannot cancel reindex task"))
	}

	tasks, err := h.tasks.ListDistributedTasks(ctx)
	if err != nil {
		return schema.NewSchemaObjectsIndexesUpdateInternalServerError().WithPayload(errorResponse(principal,
			fmt.Sprintf("listing tasks: %v", err)))
	}

	// Find the STARTED task that targets this (collection, prop, indexType).
	var target *distributedtask.Task
	var targetPayload db.ReindexTaskPayload
	for _, task := range tasks[db.ReindexNamespace] {
		if task.Status != distributedtask.TaskStatusStarted {
			continue
		}
		var payload db.ReindexTaskPayload
		if err := json.Unmarshal(task.Payload, &payload); err != nil {
			continue
		}
		if !strings.EqualFold(payload.Collection, collection) {
			continue
		}
		if !slices.Contains(payload.Properties, propertyName) {
			continue
		}
		if matches, _ := migrationTypeTargetsIndex(payload.MigrationType, indexType); !matches {
			continue
		}
		target = task
		targetPayload = payload
		break
	}

	if target == nil {
		// A live task whose payload the full decoder rejects refuses every
		// backup of this collection, and the refusal names this endpoint as
		// the remedy. The property and index type it targets are inside the
		// payload that will not decode, so the collection is all there is to
		// match on. Answering NO_OP here would leave the operator with no way
		// to clear the one task they were told to cancel.
		//
		// Runs only after the strict pass found nothing, so a decodable task
		// still wins the exact (collection, property, indexType) match.
		for _, task := range tasks[db.ReindexNamespace] {
			if task.Status != distributedtask.TaskStatusStarted {
				continue
			}
			var payload db.ReindexTaskPayload
			if err := json.Unmarshal(task.Payload, &payload); err == nil {
				continue
			}
			recovered := db.ReindexTaskCollection(task.Payload)
			if recovered == "" || !strings.EqualFold(recovered, collection) {
				continue
			}
			h.appState.Logger.WithFields(logrus.Fields{
				"audit_event": "reindex_task_cancel_unreadable_payload",
				"taskID":      task.ID,
				"collection":  collection,
				"property":    propertyName,
				"index_type":  indexType,
				"principal":   principalUsername(principal),
			}).Info("cancel: task payload will not decode; cancelling it on the collection it names")
			target = task
			// Only the collection survived the decode, so this is all the
			// drain and cleanup below get to work with.
			targetPayload = db.ReindexTaskPayload{Collection: recovered}
			break
		}
	}

	if target == nil {
		if held := h.uncancellableLiveTask(tasks[db.ReindexNamespace], collection,
			propertyName, indexType); held != nil {
			h.appState.Logger.WithFields(logrus.Fields{
				"audit_event": "reindex_task_cancel_past_cancellation_point",
				"taskID":      held.ID,
				"task_status": string(held.Status),
				"collection":  collection,
				"property":    propertyName,
				"index_type":  indexType,
				"principal":   principalUsername(principal),
			}).Info("cancel: the task has left STARTED, so DTM will not cancel it; refusing instead of NO_OP")
			return schema.NewSchemaObjectsIndexesUpdateConflict().WithPayload(errorResponse(principal,
				"cancel refused: the migration has finished building and is committing its result; "+
					"it can no longer be cancelled. Poll GET /v1/schema/<class>/indexes until every "+
					"index reports status=\"ready\"."))
		}

		// Idempotent cancel: caller's (collection, property) is known to
		// exist (updateIndex verified before dispatch). No task to cancel
		// means the request is a no-op — surface that explicitly via
		// Status: NO_OP at 202 rather than overloading 404 with two
		// distinct semantics (caller-error vs already-done).
		h.appState.Logger.WithFields(logrus.Fields{
			"audit_event": "reindex_task_cancel_noop",
			"collection":  collection,
			"property":    propertyName,
			"index_type":  indexType,
			"principal":   principalUsername(principal),
		}).Info("cancel: no in-flight task to cancel; returning NO_OP")
		return schema.NewSchemaObjectsIndexesUpdateAccepted().WithPayload(&models.IndexUpdateResponse{
			Status: reindexCancelStatusNoOp,
		})
	}

	if err := h.tasks.CancelDistributedTask(
		ctx, target.Namespace, target.ID, target.Version,
	); err != nil {
		return schema.NewSchemaObjectsIndexesUpdateInternalServerError().WithPayload(errorResponse(principal,
			fmt.Sprintf("cancelling task: %v", err)))
	}

	if provider := h.appState.ReindexProvider.Load(); provider != nil {
		// The gate is released once the handler answers, not once the cleanup
		// ends: awaitOwnerCleanupGates below still reports on this node's
		// teardown window.
		if release := h.drainAndCleanupCancelledTask(ctx, provider,
			target, &targetPayload, collection, propertyName, indexType); release != nil {
			defer release()
		}
	} else {
		h.appState.Logger.WithFields(logrus.Fields{
			"taskID":     target.ID,
			"collection": collection,
			"property":   propertyName,
			"index_type": indexType,
		}).Warn("cancel: appState.ReindexProvider is nil; skipping drain+cleanup")
	}

	// Nothing above closed a gate if this node owns none of the shards.
	h.awaitOwnerCleanupGates(ctx, &targetPayload, collection, target.ID)

	h.appState.Logger.WithFields(logrus.Fields{
		"audit_event": "reindex_task_cancelled",
		"taskID":      target.ID,
		"collection":  collection,
		"property":    propertyName,
		"index_type":  indexType,
		"principal":   principalUsername(principal),
	}).Info("reindex provider: cancelled task")

	return schema.NewSchemaObjectsIndexesUpdateAccepted().WithPayload(&models.IndexUpdateResponse{
		// The task ID embeds the qualified collection.
		TaskID: namespacing.StripOwnNamespace(principal, target.ID),
		Status: "CANCELLED",
	})
}

// reindexCleanupGateProvider is the reindex provider as the cancel path uses
// it: close the gates, wait for the local worker, and hand the gate over when
// the wait times out.
type reindexCleanupGateProvider interface {
	DrainWithCleanupGate(ctx context.Context, payload *db.ReindexTaskPayload,
		desc distributedtask.TaskDescriptor) (func(), error)
	ReleaseCleanupGateOnWorkerExit(desc distributedtask.TaskDescriptor,
		release func(), logger logrus.FieldLogger)
}

// drainAndCleanupCancelledTask drains the local reindex goroutine and then
// wipes the partial on-disk state the cancelled task left behind, with the
// backup and restore gates closed across both.
//
// The drain has to come first: the worker is still writing to the __reindex /
// __ingest buckets, and ShutdownBucket would tear them out from under it and
// corrupt the store. It is bounded so a stuck goroutine cannot turn the cancel
// into an open-ended hang; a cancel that times out still answers 202 and the
// next submit's cleanup picks the work up.
//
// Both halves run detached from the request and bounded by their own timeouts,
// so a client that disconnects after sending the cancel does not decide how far
// either half gets.
//
// It returns the gate release for the caller to defer, or nil when the drain
// timed out: the worker is then still writing, which is the case the gate
// exists for, so it is handed to
// [db.ReindexProvider.ReleaseCleanupGateOnWorkerExit] to outlive this request
// rather than dropped at its return.
func (h *indexesHandlers) drainAndCleanupCancelledTask(
	ctx context.Context,
	provider reindexCleanupGateProvider,
	target *distributedtask.Task,
	payload *db.ReindexTaskPayload,
	collection, propertyName, indexType string,
) func() {
	fields := logrus.Fields{
		"taskID":     target.ID,
		"collection": collection,
		"property":   propertyName,
		"index_type": indexType,
	}
	h.appState.Logger.WithFields(fields).Info("cancel: starting drain+cleanup for cancelled reindex task")

	// Detached from the request, and this is the wider of the two detaches on
	// this path: a disconnect here fails the drain, the handler returns before
	// the sweep, and the gate goes to the worker-exit watcher — so the sweep
	// never runs at all. What it would have healed then survives until a later
	// submit on this same node sweeps it, which may be never; the healing is
	// node-local. Its own timeout still bounds the wait.
	drainCtx, drainCancel := context.WithTimeout(
		context.WithoutCancel(ctx), reindexCancelDrainTimeout)
	releaseGate, drainErr := provider.DrainWithCleanupGate(drainCtx, payload, target.TaskDescriptor)
	drainCancel()
	if drainErr != nil {
		provider.ReleaseCleanupGateOnWorkerExit(target.TaskDescriptor, releaseGate, h.appState.Logger)
		h.appState.Logger.WithFields(fields).Errorf(
			"cancel: timed out waiting for local reindex goroutine to drain (%v); skipping inline cleanup — next submit will retry", drainErr)
		return nil
	}

	// The gate reaches the caller through the return value below, so a panic in
	// the sweep would unwind past it and leave the caller nothing to defer —
	// backups and restores of this collection stay refused until the process is
	// restarted. Release it here on the way out and re-panic. OnceFunc so the
	// caller's own defer over the same function stays a no-op; the release is
	// refcounted and a second call would open a gate somebody else holds.
	release := sync.OnceFunc(releaseGate)
	defer func() {
		if r := recover(); r != nil {
			release()
			panic(r)
		}
	}()

	h.appState.Logger.WithFields(fields).Info("cancel: drain complete, running on-disk cleanup")
	// Wipe the sidecars and migration directories for every indexType this
	// migration touches — change-tokenization spawns both a searchable and a
	// filterable strategy under one task, so cleaning only the URL's indexType
	// leaves the sibling orphaned. Errors are logged; submit-time pre-cleanup
	// will retry.
	indexTypesToClean, known := indexTypesFromMigrationType(payload.MigrationType)
	if !known || len(indexTypesToClean) == 0 {
		indexTypesToClean = []string{indexType}
	}
	// Detached from the request: this sweep is the only trigger we control for
	// state nothing else clears, so aborting it because the client went away
	// just defers the work to a later submit that may never come, and a
	// disconnect part-way through leaves the sidecar buckets deregistered with
	// started.mig still on disk. The timeout below keeps it bounded.
	//
	// The gate released below does NOT mean "the disk is clean": it reports
	// cleanup-in-progress, and a failed sweep is logged with the gate released
	// anyway. Recovery is the next submit's pre-cleanup and the restart audit.
	// See weaviate/0-weaviate-issues#352.
	cleanupCtx, cancelCleanup := context.WithTimeout(
		context.WithoutCancel(ctx), reindexCancelCleanupTimeout)
	defer cancelCleanup()

	var cleanupErrs []error
	for _, it := range indexTypesToClean {
		if err := h.appState.DB.CleanStalePartialReindexState(cleanupCtx, collection, propertyName, it); err != nil {
			cleanupErrs = append(cleanupErrs, fmt.Errorf("indexType=%q: %w", it, err))
		}
	}
	if len(cleanupErrs) > 0 {
		h.appState.Logger.WithFields(fields).WithField("strategies", indexTypesToClean).Errorf(
			"cancel: cleaning partial reindex state on disk for %d strategies failed: %v; next submit's defense-in-depth cleanup will retry",
			len(cleanupErrs), cleanupErrs)
	} else {
		h.appState.Logger.WithFields(fields).Info("cancel: on-disk cleanup complete")
	}
	return release
}

// reindexCancelCleanupTimeout bounds the on-disk sweep once it is detached from
// the request. Generous: it is bucket teardown across every strategy the
// migration touched, and abandoning it half-done is what the detach avoids.
const reindexCancelCleanupTimeout = 2 * time.Minute

// uncancellableLiveTask finds a task that the backup gate and the status
// endpoint both count as live ([db.IsLiveReindexTaskStatus]) but that
// [distributedtask.Manager.CancelTask] will not accept, because it has left
// STARTED. PREPARING and SWAPPING are those states, and so is any status a
// newer node introduced.
//
// Three predicates used to disagree about one task: the gate refused backups of
// its collection, the status endpoint reported it running, and cancel answered
// NO_OP. The operator was told to cancel and then told there was nothing to
// cancel, with the backup still refused. Widening the cancel filter is not the
// fix — DTM rejects a cancel in these states, so that only turns the NO_OP into
// a 500. What is wrong is the answer, so this exists to give the caller a
// refusal that matches the gate.
//
// Matching mirrors the two passes above: an exact (collection, property, index
// type) match where the payload decodes, and a collection-only match where it
// does not, because the property is inside the payload that will not decode.
func (h *indexesHandlers) uncancellableLiveTask(tasks []*distributedtask.Task,
	collection, propertyName, indexType string,
) *distributedtask.Task {
	for _, task := range tasks {
		if task.Status == distributedtask.TaskStatusStarted ||
			!db.IsLiveReindexTaskStatus(task.Status) {
			continue
		}
		var payload db.ReindexTaskPayload
		if err := json.Unmarshal(task.Payload, &payload); err != nil {
			if recovered := db.ReindexTaskCollection(task.Payload); recovered != "" &&
				strings.EqualFold(recovered, collection) {
				return task
			}
			continue
		}
		if !strings.EqualFold(payload.Collection, collection) {
			continue
		}
		if !slices.Contains(payload.Properties, propertyName) {
			continue
		}
		if matches, _ := migrationTypeTargetsIndex(payload.MigrationType, indexType); !matches {
			continue
		}
		return task
	}
	return nil
}

// reindexCancelStatusNoOp is the IndexUpdateResponse.Status value the
// cancel handler emits when there is nothing to cancel. Lets scripts
// treat "cancel was a no-op" and "cancel cancelled an in-flight task"
// as a single success path rather than the previous "200 vs 404"
// disambiguation — see the cancelReindexTask godoc.
const reindexCancelStatusNoOp = "NO_OP"

// reindexCancelDrainTimeout caps how long the cancel handler waits for
// the local reindex goroutine to exit before falling back to "let the
// next submit clean up". 10s matches the DTM scheduler's analogous
// waits and is comfortably above the per-iteration cycle (which checks
// ctx.Err() every checkProcessingEveryNoObjects=1000 objects, with a
// processingDuration cap of 600s but a per-iteration cap that's much
// shorter in practice — empirically <1s on test corpora).
const reindexCancelDrainTimeout = 10 * time.Second

// finalizeWindowMin / finalizeWindowMax bound the "FINISHED but
// flag-off → indexing@100%" override in [mergeReindexStatus]. The
// window is normally computed as 2× the DTM scheduler tick interval,
// but is clamped at both ends:
//
//   - finalizeWindowMin (3s) protects against pathological sub-second
//     tick configs where 2× would shrink the legitimate window faster
//     than realistic swap-phase jitter. 3s comfortably covers the
//     in-test 1s tick + swap + jitter.
//
//   - finalizeWindowMax (10s) caps how long a stale FINISHED task can
//     bleed an "indexing(1)" pill after a DELETE — production tick is
//     60s, so a naive 2× would let the bleed live for 2 minutes,
//     which was the user-visible face of https://github.com/weaviate/weaviate/issues/10675.
//
// Outside the window, flagOn==false cannot legitimately mean "swap
// pending" — either the swap failed silently (logged as "swap
// INCOMPLETE" elsewhere) or the swap completed and DELETE flipped the
// flag back to false (the frontend repro on 2026-05-14 in
// https://github.com/weaviate/weaviate/issues/10675 — "indexing(1) bleed"). In both cases
// surfacing the override would be a status lie. The trade-off in
// production: between task FINISHED and the schema flag flip, a
// caller polling the GET endpoint will see "indexing@100%" for up to
// 10s, then briefly see an empty searchable entry, then see "ready"
// once the flag flips. The brief empty entry is the original UX gap
// that the override was added to bridge (fd4bfab7cb); we accept it
// here as the lesser evil compared to the unbounded bleed.
const (
	finalizeWindowMin = 3 * time.Second
	finalizeWindowMax = 10 * time.Second
)

// indexTypesFromMigrationType returns the canonical inverted-index types
// ("filterable", "searchable", "rangeable") that a migration type targets,
// for use by submit-time pre-cleanup. Returns (nil, false) only for unknown
// migration types — every known type returns at least one indexType.
//
// Most migration types target exactly one index. change-tokenization (both
// indexes) targets TWO — it spawns one ShardReindexTaskGeneric per index
// (searchable + filterable) via createReindexTasks, and each leaves its own
// .migrations/<prefix>_<prop>/ sentinel directory on disk. Pre-submit
// cleanup must wipe BOTH dirs; cleaning only one of them was the root cause
// of the Sev 1 data-loss bug fixed alongside this change (see Journey 7 in
// change_tok_delete_journeys_test.go): a prior filterable-only retokenize
// left .migrations/filterable_retokenize_<prop>/tidied.mig on disk, the
// next change-tokenization-both submit did not clean it, and its
// FilterableRetokenize sub-task short-circuited on OnAfterLsmInit's
// IsTidied check while OnMigrationComplete still flipped the schema's
// Tokenization. Schema and on-disk state then disagreed.
//
// Callers iterate the returned slice and run CleanStalePartialReindexState
// once per indexType. Safe to call when no stale state exists: missing
// directories and unloaded buckets are silently skipped.
func indexTypesFromMigrationType(mt db.ReindexMigrationType) ([]string, bool) {
	switch mt {
	case db.ReindexTypeEnableSearchable, db.ReindexTypeChangeAlgorithm, db.ReindexTypeRebuildSearchable:
		return []string{"searchable"}, true
	case db.ReindexTypeEnableFilterable, db.ReindexTypeRepairFilterable:
		return []string{"filterable"}, true
	case db.ReindexTypeEnableRangeable, db.ReindexTypeRepairRangeable:
		return []string{"rangeable"}, true
	case db.ReindexTypeChangeTokenization:
		// change-tokenization-both runs ONE task per inverted index
		// (searchable + filterable). Each leaves its own per-property
		// migration dir on disk. Pre-cleanup must wipe both, otherwise a
		// stale tidied.mig from a previous single-index retokenize on the
		// same prop short-circuits the sub-task and produces a schema /
		// bucket state mismatch (Sev 1 silent data loss).
		return []string{"searchable", "filterable"}, true
	case db.ReindexTypeChangeTokenizationFilterable:
		return []string{"filterable"}, true
	}
	return nil, false
}

// migrationTypeTargetsIndex returns:
//
//   - matches: true if the migration type writes to the named index bucket.
//   - isKnown: true if the migration type is one this function knows about.
//
// A new ReindexType added to the codebase without being mapped here would
// return (false, false). Callers that need to log/alert on that case can
// check the second return; cancel-path callers can ignore it because a
// (false, false) result still means "this task is not a cancel target".
func migrationTypeTargetsIndex(mt db.ReindexMigrationType, indexType string) (matches, isKnown bool) {
	switch mt {
	case db.ReindexTypeEnableSearchable, db.ReindexTypeChangeAlgorithm, db.ReindexTypeRebuildSearchable:
		return indexType == "searchable", true
	case db.ReindexTypeEnableFilterable, db.ReindexTypeRepairFilterable:
		return indexType == "filterable", true
	case db.ReindexTypeEnableRangeable, db.ReindexTypeRepairRangeable:
		return indexType == "rangeable", true
	case db.ReindexTypeChangeTokenization:
		// touches both searchable and filterable buckets
		return indexType == "searchable" || indexType == "filterable", true
	case db.ReindexTypeChangeTokenizationFilterable:
		return indexType == "filterable", true
	}
	return false, false
}

// parsedReindexTask pairs a distributed task with its already-unmarshalled
// reindex payload. The handler builds a slice of these once per request
// so mergeReindexStatus doesn't re-unmarshal task.Payload N times where
// N is the number of properties in the collection.
type parsedReindexTask struct {
	task    *distributedtask.Task
	payload db.ReindexTaskPayload
	// unreadable marks a live task the full decoder rejected. Only
	// payload.Collection is populated, recovered by
	// [db.ReindexTaskCollection]; every other field is zero.
	unreadable bool
}

// parseReindexTasks unmarshals every reindex task's payload once.
//
// A live task whose payload the full decoder rejects is kept, flagged
// unreadable, with just the collection recovered. The backup gate refuses
// that whole collection on exactly this payload and tells the operator to
// poll here until every index reads "ready", so dropping the task would
// answer "ready" for a collection backups keep refusing. A rolling upgrade
// that retypes a payload field produces exactly that payload. A terminal
// task is still dropped: it blocks nothing.
//
// FINISHED tasks are kept in the slice (they were dropped here historically,
// but mergeReindexStatus now uses them to surface a brief "indexing@100%"
// SWAPPING-window entry while OnGroupCompleted's swap propagates to the
// schema — without that, the GET response goes empty for a few ms between
// FINISHED and the schema flip, which renders as "None" in the UI).
func parseReindexTasks(tasks []*distributedtask.Task) []parsedReindexTask {
	parsed := make([]parsedReindexTask, 0, len(tasks))
	for _, task := range tasks {
		var payload db.ReindexTaskPayload
		if err := json.Unmarshal(task.Payload, &payload); err != nil {
			collection := db.ReindexTaskCollection(task.Payload)
			if collection == "" || !db.IsLiveReindexTaskStatus(task.Status) {
				continue
			}
			parsed = append(parsed, parsedReindexTask{
				task:       task,
				payload:    db.ReindexTaskPayload{Collection: collection},
				unreadable: true,
			})
			continue
		}
		parsed = append(parsed, parsedReindexTask{task: task, payload: payload})
	}
	return parsed
}

// mergeReindexStatus checks if there's an active or recently-terminated
// reindex task that targets the given property+indexType and updates the
// IndexStatus accordingly.
//
// Status values produced (in addition to the caller-supplied default
// "ready"):
//
//   - "pending":    STARTED task, no unit progress yet.
//   - "indexing":   STARTED task with some progress, OR a FINISHED task
//     whose swap hasn't propagated to the schema flag yet
//     (the brief OnGroupCompleted finalize window). The
//     `flagOn` parameter distinguishes the two: when the
//     schema flag is already on, a stale FINISHED task is
//     ignored — the base "ready" wins.
//   - "failed":     latest matching task ended in FAILED.
//   - "cancelled":  latest matching task ended in CANCELLED.
//
// `flagOn` is the caller's view of whether the corresponding schema flag
// (IndexFilterable / IndexSearchable / IndexRangeFilters, depending on
// indexType) is currently true. It lets this function decide whether a
// FINISHED task is "still finalizing" (flag-off) or "fully done"
// (flag-on, so the base "ready" entry takes over).
//
// Property matching is uniform across all migration types: every branch
// requires payload.Properties to be non-empty and to contain propName.
// The REST handler always populates Properties with exactly one entry;
// rejecting an empty list consistently guards against direct cluster
// payload authoring fanning out a synthetic "indexing" entry to every
// property in the collection (a hazard that would otherwise be specific
// to the repair-* migration types if they accepted an empty list as
// "match all").
//
// The logger is used to flag unknown migration types: a future ReindexType
// added without updating this switch would otherwise silently report "ready"
// for an in-flight task. Passing a nil logger is allowed (test callers may
// rely on this); the entry is still skipped, just without a log line.
// finalizeWindow caps the "FINISHED-but-flag-off → indexing@100%"
// override (see the TaskStatusFinished branch below). Callers pass in
// 2× the DTM scheduler tick interval (clamped to finalizeWindowMin);
// the test harness passes a wider value because the test container
// always uses 1s ticks. Pass 0 to disable the override entirely (rare;
// kept for tests that want to assert the post-DELETE bleed never
// surfaces regardless of FinishedAt freshness).
func mergeReindexStatus(idx *models.IndexStatus, collection, propName, indexType string, flagOn bool, parsedTasks []parsedReindexTask, finalizeWindow time.Duration, logger logrus.FieldLogger) {
	// Two tasks for the same (collection, prop, indexType) may coexist —
	// e.g. a freshly retried STARTED enable-filterable plus the original
	// FAILED attempt that the operator just retried (terminal tasks
	// deliberately do NOT block fresh submits; see checkReindexConflict).
	// Pick the most useful one to surface rather than first-in-map-order:
	//   STARTED  > FINISHED ≈ FAILED ≈ CANCELLED  (in-flight beats terminal)
	//   newer StartedAt > older StartedAt          (within the same priority)
	// FINISHED tasks are in the slice too: parseReindexTasks keeps them so
	// the finalize window below can surface the swap that has not yet
	// reached the schema flag.
	var best *distributedtask.Task
	var bestPayload db.ReindexTaskPayload
	for _, pt := range parsedTasks {
		task := pt.task
		payload := pt.payload

		if pt.unreadable {
			// Nothing but the collection decoded, so none of the matching
			// below can be answered. Handled after the loop.
			continue
		}

		if !strings.EqualFold(payload.Collection, collection) {
			continue
		}

		// Require a non-empty Properties list. The REST handler always
		// populates this with one entry; an empty list only happens via
		// direct cluster payload authoring and is treated as "match
		// nothing" so we never silently fan out a synthetic entry to
		// every property in the collection.
		if !slices.Contains(payload.Properties, propName) {
			continue
		}

		targets, known := migrationTypeTargetsIndex(payload.MigrationType, indexType)
		if !known && logger != nil {
			// A new ReindexType was added without being mapped to a bucket,
			// which would silently report "ready" for an in-flight task. Log
			// loudly so this surfaces in CI/staging before it hits prod.
			// targets is false here too, so we fall through and leave the
			// synthetic entry alone.
			logger.WithFields(logrus.Fields{
				"migration_type": payload.MigrationType,
				"task_id":        task.ID,
				"collection":     collection,
			}).Errorf("reindex status: unknown migration type %q; index status may be stale", payload.MigrationType)
		}
		if !targets {
			continue
		}

		if best == nil || taskStatusPriority(task) > taskStatusPriority(best) ||
			(taskStatusPriority(task) == taskStatusPriority(best) && task.StartedAt.After(best.StartedAt)) {
			best = task
			bestPayload = payload
		}
	}

	if best == nil {
		markUnreadablePayload(idx, collection, parsedTasks)
		return
	}

	// Decide the status first; only THEN apply per-migration-type side
	// effects (Tokenization / TargetTokenization / TargetAlgorithm). Setting
	// those fields ahead of the status decision was the source of the
	// "post-FINISHED targetAlgorithm bleed" bug: for a RepairSearchable task
	// that has FINISHED with the schema flag already flipped (UsingBlockMaxWAND
	// == true), the status switch correctly leaves the entry as the base
	// "ready", but the unconditional TargetAlgorithm assignment above had
	// already poisoned the response with an in-flight signal that no longer
	// applies. The post-rebuild contract (verified by
	// TestSingleNode_ReindexSuite/MapToBlockmax) is: once the schema flag
	// has caught up, the synthetic "targetAlgorithm" / "targetTokenization"
	// fields must be empty.
	//
	// The rule is: a side-effect field is surfaced only when the status
	// switch below changes idx.Status away from "ready" (i.e., we are
	// actually painting an in-flight or SWAPPING-window signal). When the
	// status stays "ready", we keep idx in its base state.
	surfaceSyntheticFields := false

	switch best.Status {
	case distributedtask.TaskStatusFailed:
		idx.Status = "failed"
		idx.Progress = aggregateProgress(best)
		surfaceSyntheticFields = true
	case distributedtask.TaskStatusCancelled:
		idx.Status = "cancelled"
		idx.Progress = aggregateProgress(best)
		surfaceSyntheticFields = true
	case distributedtask.TaskStatusStarted:
		progress := aggregateProgress(best)
		idx.Progress = progress
		// Any non-PENDING unit means work has started somewhere; flip the
		// pill to "indexing" without waiting for the first throttled
		// progress checkpoint (which can lag by tens of seconds on a large
		// shard while per-shard setup drains).
		if progress > 0 || anyUnitWorking(best) {
			idx.Status = "indexing"
		} else {
			idx.Status = "pending"
		}
		surfaceSyntheticFields = true
	case distributedtask.TaskStatusPreparing, distributedtask.TaskStatusSwapping:
		// Units done; cross-replica PREP barrier or per-node swap still in
		// flight. Surface as "indexing at 100%" until FINISHED + flagOn.
		idx.Status = "indexing"
		idx.Progress = 1.0
		surfaceSyntheticFields = true
	case distributedtask.TaskStatusFinished:
		// The DTM declares a task FINISHED once every unit is terminal, but
		// for semantic migrations (enable-*, change-tokenization) the actual
		// schema flag flip happens later, inside OnGroupCompleted's swap
		// phase. Without a synthetic entry, that window — from "task
		// FINISHED" to "schema flag flipped on this node" — would leave the
		// GET response with no synthetic entry at all and no base "ready"
		// entry (because the flag is still off), so the UI would see an
		// empty `indexes` array and render "None".
		// Treat it as "indexing@100%" until the schema catches up; once
		// flagOn flips true, the base case "ready" override takes precedence
		// and this branch is effectively ignored.
		//
		// Bound the window by task.FinishedAt: outside it, flagOn==false
		// cannot mean "swap pending" — the swap window is at most one
		// scheduler tick plus per-shard swap time, comfortably under
		// reindexFinalizeWindow. If flagOn is still false past this
		// window, the only realistic causes are:
		//   - the swap completed (flag flipped true) and a subsequent
		//     DELETE flipped it back to false (the frontend repro on
		//     2026-05-14 #10675 — "indexing(1) bleed");
		//   - the swap failed silently (logged loudly by
		//     OnGroupCompleted's "swap INCOMPLETE" branch).
		// In neither case do we want a synthetic "indexing@100%" entry —
		// the first case is a stale-task false signal, the second is an
		// error condition the swap-incomplete logs already surface.
		if !flagOn && finalizeWindow > 0 && time.Since(best.FinishedAt) < finalizeWindow {
			idx.Status = "indexing"
			idx.Progress = 1.0
			surfaceSyntheticFields = true
		}
	}

	// A matching task that leaves the entry at "ready" answers nothing the
	// unreadable payload has not already invalidated, so the fallback still
	// applies. The switch above reaches "ready" for a FINISHED task the
	// schema has caught up with, and for a status this build does not know.
	// A FINISHED task survives for DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS
	// (5 days by default), so this is not a narrow window.
	if idx.Status == models.IndexStatusStatusReady &&
		markUnreadablePayload(idx, collection, parsedTasks) {
		return
	}

	// Only paint the per-migration-type "in-flight" side-effect fields when
	// the status switch actually surfaced an in-flight or finalizing signal.
	// If the entry stayed "ready" (FINISHED + flag-on, or FINISHED outside
	// the finalize window), the migration has either completed and propagated
	// to the schema (the schema-derived fields above are authoritative) or
	// the task is stale and shouldn't pollute the response.
	if !surfaceSyntheticFields {
		return
	}

	switch bestPayload.MigrationType {
	case db.ReindexTypeEnableSearchable:
		if bestPayload.TargetTokenization != "" {
			idx.Tokenization = bestPayload.TargetTokenization
		}
	case db.ReindexTypeChangeTokenization,
		db.ReindexTypeChangeTokenizationFilterable:
		if bestPayload.TargetTokenization != "" {
			idx.TargetTokenization = bestPayload.TargetTokenization
		}
	case db.ReindexTypeChangeAlgorithm:
		// repair-searchable migrates WAND → BlockMax. The targetAlgorithm
		// lets the UI render the in-flight switch the same way it renders
		// targetTokenization for change-tokenization.
		idx.TargetAlgorithm = models.IndexStatusTargetAlgorithmBlockmax
	case db.ReindexTypeRebuildSearchable,
		db.ReindexTypeRepairFilterable,
		db.ReindexTypeEnableFilterable, db.ReindexTypeEnableRangeable,
		db.ReindexTypeRepairRangeable:
		// No tokenization or algorithm side effects for these types.
	}
}

// markUnreadablePayload paints the entry as pending when a live task whose
// payload will not decode names this collection, and reports whether it did.
//
// Such a task holds the whole collection at the backup gate, and the refusal
// sends the operator here to poll until every index reads "ready". Reporting
// "ready" would send them back to a backup that keeps being refused.
//
// Every index of the collection carries it because the property and index type
// it targets are inside the payload that will not decode. That matches the
// gate, which blocks the collection for the same reason. Progress is left at
// zero: none was readable.
func markUnreadablePayload(idx *models.IndexStatus, collection string, parsedTasks []parsedReindexTask) bool {
	for _, pt := range parsedTasks {
		if pt.unreadable && strings.EqualFold(pt.payload.Collection, collection) {
			idx.Status = models.IndexStatusStatusPending
			idx.Progress = 0
			return true
		}
	}
	return false
}

// taskStatusPriority returns a priority for picking the most user-relevant
// task when more than one task matches a (collection, prop, indexType).
// In-flight beats terminal: a user who has just retried a previously
// failed migration wants to see the new attempt's progress, not the old
// failure. FINISHED ranks alongside FAILED / CANCELLED so a recently-
// completed FINISHED task wins the StartedAt tiebreak over an older
// FAILED on the same property (and mergeReindexStatus uses it to keep
// the synthetic "indexing@100%" entry visible until the schema flip
// propagates — see the FINISHED case there).
func taskStatusPriority(task *distributedtask.Task) int {
	switch task.Status {
	case distributedtask.TaskStatusStarted,
		distributedtask.TaskStatusPreparing,
		distributedtask.TaskStatusSwapping:
		// PREPARING and SWAPPING rank alongside STARTED: from the user's
		// perspective the task is still running (PREP barrier or swap
		// pending; schema flip has not yet committed). Surface their
		// synthetic "indexing@100%" entry instead of an older FAILED
		// attempt's terminal entry.
		return 2
	case distributedtask.TaskStatusFailed,
		distributedtask.TaskStatusCancelled,
		distributedtask.TaskStatusFinished:
		return 1
	default:
		return 0
	}
}

// aggregateProgress averages Unit.Progress across all units in the task.
// Returns 0 when there are no units.
func aggregateProgress(task *distributedtask.Task) float32 {
	if len(task.Units) == 0 {
		return 0
	}
	var total float32
	for _, u := range task.Units {
		total += u.Progress
	}
	return total / float32(len(task.Units))
}

// anyUnitWorking returns true if at least one unit has transitioned out
// of PENDING — i.e. some shard is actively iterating, has finished, or
// failed.
func anyUnitWorking(task *distributedtask.Task) bool {
	for _, u := range task.Units {
		if u.Status != distributedtask.UnitStatusPending {
			return true
		}
	}
	return false
}

func dataTypeString(prop *models.Property) string {
	if len(prop.DataType) > 0 {
		return prop.DataType[0]
	}
	return ""
}

func shortRandomSuffix() string {
	b := make([]byte, 2) // 4 hex chars
	if _, err := rand.Read(b); err != nil {
		return "0000"
	}
	return hex.EncodeToString(b)
}

// isSyntheticStatus reports whether the IndexStatus.Status value was
// emitted by mergeReindexStatus (i.e. driven by a reindex task) and so
// should be surfaced even when the property's schema flag for that index
// type is off. The default "ready" remains invisible when the flag is
// off, since it carries no actionable signal.
func isSyntheticStatus(s string) bool {
	switch s {
	case models.IndexStatusStatusIndexing,
		models.IndexStatusStatusPending,
		models.IndexStatusStatusFailed,
		models.IndexStatusStatusCancelled:
		return true
	}
	return false
}

func errorResponse(principal *models.Principal, msg string) *models.ErrorResponse {
	return &models.ErrorResponse{
		Error: []*models.ErrorResponseErrorItems0{
			{Message: namespacing.StripErrorMessage(principal, msg)},
		},
	}
}

// normalizeSearchableAlgorithm canonicalises the BM25-algorithm string the
// caller sent on a PUT /v1/schema/{class}/indexes/{prop} body. Returns the
// lowercase model constant ("wand" / "blockmax") when the input is a
// recognised alias, or "" when it isn't.
//
// Swagger's EnumCase validator is case-insensitive but otherwise rigid: it
// would already reject "block_max" or "blockmaxwand" at the binding layer.
// We re-canonicalise here for two reasons:
//
//  1. Defence in depth — if the swagger spec is ever loosened (e.g. to add
//     a new algorithm) the dispatcher still applies a strict allowlist
//     against the canonical value rather than an EqualFold against a single
//     hard-coded enum constant.
//  2. Operationally desired aliases — we accept "block-max" / "block_max"
//     / "BlockMaxWAND" because callers in the wild have written them; the
//     intent is unambiguous and rejecting on a punctuation difference is
//     hostile UX. The accepted alias set is intentionally small and
//     closed; new aliases require an explicit code change here.
func normalizeSearchableAlgorithm(s string) string {
	// Strip surrounding whitespace before any other transform — a body
	// like {"algorithm":" blockmax "} should not be rejected on a stray
	// space.
	trimmed := strings.TrimSpace(s)
	lower := strings.ToLower(trimmed)
	// Strip ASCII separators that callers sometimes inject (e.g.
	// "block-max", "block_max"). Done after lowercasing so the set is
	// minimal.
	stripped := strings.ReplaceAll(strings.ReplaceAll(lower, "-", ""), "_", "")
	switch stripped {
	case "blockmax", "blockmaxwand", "bmw":
		return models.IndexStatusAlgorithmBlockmax
	case "wand":
		return models.IndexStatusAlgorithmWand
	}
	return ""
}

// maxConcurrentReindexPerCollection caps how many STARTED reindex tasks
// can target the same collection at once. Each task creates ingest +
// backup buckets on every replica; without a cap, a script that runs
// PUT /indexes/<prop> per property would fan out N tasks for an
// N-property collection and overwhelm both LSM compaction and disk.
//
// The value is sized to comfortably accommodate realistic batch property
// changes (e.g. retokenizing every text property on a ~20-property
// collection in one go) while still preventing pathological unbounded
// fan-out from a script that loops over hundreds of properties. The
// original value of 4 was too restrictive: it rejected legitimate batch
// migrations against modest-sized collections and broke the
// reindex_concurrent acceptance test which exercises 15 simultaneous
// non-conflicting submits.
const maxConcurrentReindexPerCollection = 32

// reindexCapExceededResponder returns a 429 Too Many Requests response with
// the standard ErrorResponse body shape. The swagger spec for
// PUT /v1/schema/{class}/indexes/{prop} does not declare a 429 response —
// it predates the per-collection cap — so we hand-roll the responder
// instead of adding to the generated code.
//
// The status is intentionally 429 and not 503: the rejection is driven by
// a concurrency limit specific to this caller's collection, not by the
// cluster being unavailable. Returning 503 misled monitoring (and the
// reindex_concurrent acceptance test asserts the cap is reached, not that
// the service went unhealthy).
func reindexCapExceededResponder(principal *models.Principal, collection string, inflight, capLimit int) middleware.Responder {
	body := errorResponse(principal, fmt.Sprintf(
		"collection %q already has %d concurrent reindex tasks (max %d); wait for one to finish before submitting another",
		collection, inflight, capLimit))
	return middleware.ResponderFunc(func(w http.ResponseWriter, producer runtime.Producer) {
		w.WriteHeader(http.StatusTooManyRequests)
		if err := producer.Produce(w, body); err != nil {
			// Match the generated swagger responders' behaviour for body
			// write failures; the recovery middleware logs and returns 500.
			panic(err)
		}
	})
}

// countStartedTasksForCollection counts in-flight reindex tasks for a
// collection. Counts every non-terminal status (STARTED/PREPARING/SWAPPING
// via IsActive) because PREPARING/SWAPPING still hold tracker dirs and
// reindex buckets.
func countStartedTasksForCollection(collection string, tasks []*distributedtask.Task) int {
	n := 0
	for _, task := range tasks {
		if !task.Status.IsActive() {
			continue
		}
		var payload db.ReindexTaskPayload
		if err := json.Unmarshal(task.Payload, &payload); err != nil {
			continue
		}
		if strings.EqualFold(payload.Collection, collection) {
			n++
		}
	}
	return n
}

// checkReindexConflict checks if a new reindex task would conflict with any
// running tasks. Returns (reason, nil) when no conflict, ("reason", nil)
// when a conflict is detected, or ("", err) when a running task has a
// payload we cannot decode — in which case we cannot prove non-conflict
// and the caller must reject the submit.
//
// Two tasks conflict if they touch the same index bucket type for the same
// property. Every migration type is property-scoped: the property the task
// targets is the one named in payload.Properties. An empty Properties list
// is reserved for a future whole-collection rebuild and is treated as
// matching any property for conflict purposes.
//
// The bucket types each migration touches on its targeted property:
//   - repair-searchable:    searchable bucket
//   - repair-filterable:    filterable bucket
//   - enable-searchable:    searchable bucket (from scratch)
//   - enable-filterable:    filterable bucket (from scratch)
//   - change-tokenization:  searchable + filterable buckets
//   - enable-rangeable:     rangeable bucket — no cross-type conflicts
//
// Unparseable payloads (e.g. payload schema change across versions, RAFT
// replay of a task from an older binary) are treated as a hard error
// rather than silently skipped: silent-skip would let a real bucket-level
// conflict slip through and allow a second task to race against the
// in-flight one.
func checkReindexConflict(collection string, newType db.ReindexMigrationType,
	newProps []string, tasks []*distributedtask.Task,
) (string, error) {
	for _, task := range tasks {
		if !task.Status.IsActive() {
			continue
		}

		var payload db.ReindexTaskPayload
		if err := json.Unmarshal(task.Payload, &payload); err != nil {
			return "", fmt.Errorf(
				"in-flight reindex task %q has an unparseable payload; cannot verify conflict; "+
					"retry after operator inspects the task: %w", task.ID, err)
		}
		// Successfully parsed but informationally empty: a `{}` payload, or
		// one missing Collection / MigrationType. This is the same epistemic
		// state as unparseable — we cannot prove non-conflict — so we
		// refuse for the same reason. Most realistic cause: an older binary
		// wrote a payload shape we no longer recognize and the missing fields
		// dropped to their zero values during Unmarshal.
		if payload.Collection == "" || payload.MigrationType == "" {
			return "", fmt.Errorf(
				"in-flight reindex task %q has an empty Collection or MigrationType "+
					"(payload may have been written by an older binary); cannot verify conflict; "+
					"retry after operator inspects the task", task.ID)
		}
		if !strings.EqualFold(payload.Collection, collection) {
			continue
		}

		if conflict := db.TypesConflictReason(newType, newProps, payload.MigrationType, payload.Properties); conflict != "" {
			return fmt.Sprintf("reindex task %q conflicts: %s", task.ID, conflict), nil
		}
	}
	return "", nil
}

// The conflict predicate + bucket-touch helpers + property-overlap
// helper used by the pre-flight check above all live in the db
// package now ([db.TypesConflictReason], [db.TouchesSearchable],
// [db.TouchesFilterable], [db.ReindexPropsOverlap]) — they're shared
// with the FSM-deterministic conflict check at apply time so the two
// paths can't drift on what counts as a conflict.
