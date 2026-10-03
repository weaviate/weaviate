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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/multitenancy"
)

// ttlTenantsManager is the subset of schemaUC.TenantsActivityManager used by tenantTTLLoop.
type ttlTenantsManager interface {
	TenantsStatus(class string, tenants ...string) (map[string]string, error)
	DeactivateTenants(ctx context.Context, class string, tenants ...string) error
}

// ttlDeactivateTimeout bounds the deferred DeactivateTenants RAFT call so that a stuck or
// partitioned leader cannot block the goroutine indefinitely.
const ttlDeactivateTimeout = 30 * time.Second

// tenantTTLLoop manages the TTL deletion loop for a single multi-tenant shard.
//
// When autoActivationEnabled is true and the tenant was COLD at the start of the loop, the
// loop guarantees that DeactivateTenants is called on exit — even if ctx is canceled — via a
// deferred call with a bounded timeout. This prevents a previously-COLD tenant from being
// left permanently HOT when a concurrent RAFT operation cancels the TTL context mid-deletion.
type tenantTTLLoop struct {
	class, tenant         string
	autoActivationEnabled bool
	mgr                   ttlTenantsManager
	findUUIDs             func(ctx context.Context) ([]strfmt.UUID, error)
	processBatch          func(ctx context.Context, uuids []strfmt.UUID) (deleted bool, err error)

	// set once this tenant's failure is filed, so a tenant retried across rounds files it
	// once for the sweep rather than once a round
	filed bool
}

// errTTLNoProgress reports a batch that deleted nothing without failing. The tenant or
// shard that produced it is not swept again for the rest of that sweep, because the next
// round would find the same uuids.
var errTTLNoProgress = errors.New("no object deleted and no error reported, not swept again until the next sweep")

// ttlFailureReason names what a finished batch should file, or nil where there is nothing to
// file. A stopped sweep also deletes nothing, which is not the shard or tenant failing.
func ttlFailureReason(ctx context.Context, deleted bool, err error) error {
	if err != nil || deleted || context.Cause(ctx) != nil {
		return err
	}
	return errTTLNoProgress
}

// shardIsLazyUnloaded reports whether the named shard is a lazy shard not yet materialized.
// Only HOT tenants are lazy shards, so this never hides a COLD tenant from auto-activation.
func (i *Index) shardIsLazyUnloaded(shardName string) bool {
	lazy, ok := i.shards.Load(shardName).(*LazyLoadShard)
	return ok && !lazy.isLoaded()
}

// deleteFromShards calls deleteShard once per shard that has expired uuids and was not dropped
// earlier in the sweep, and waits unless the sweep ends first, which it reports as stopped. A shard
// that deleted nothing is dropped for the rest of the sweep, and one that failed is filed once for
// it rather than once a round. It returns whether any shard deleted, then whether the sweep stopped.
// On a stopped return a shard handed to the group may still be recording, so dropped and filed are
// complete only once the caller has waited on the group.
func deleteFromShards(ctx context.Context, eg *enterrors.ErrorGroupWrapper,
	ec errorcompounder.ErrorCompounder, class string, shards2uuids map[string][]strfmt.UUID,
	dropped, filed map[string]struct{},
	deleteShard func(shard string, uuids []strfmt.UUID) (bool, error),
) (bool, bool) {
	shards := make([]string, 0, len(shards2uuids))
	for shard, uuids := range shards2uuids {
		if _, skip := dropped[shard]; !skip && len(uuids) > 0 {
			shards = append(shards, shard)
		}
	}

	var (
		anyDeleted atomic.Bool
		shardsLock sync.Mutex
	)
	// returned is false when the delete panicked: the group files that panic, so record only drops the shard
	record := func(shard string, deleted bool, err error, returned bool) {
		var reason error
		if returned {
			reason = ttlFailureReason(ctx, deleted, err)
		}

		shardsLock.Lock()
		defer shardsLock.Unlock()

		_, alreadyFiled := filed[shard]
		if reason != nil && !alreadyFiled {
			ec.AddGroups(reason, class, shard)
			filed[shard] = struct{}{}
		}

		if deleted {
			anyDeleted.Store(true)
			return
		}
		// the search cannot exclude its uuids, so a later round would hand it the same ones
		dropped[shard] = struct{}{}
	}

	wg := new(sync.WaitGroup)
	for idx, shard := range shards {
		uuids := shards2uuids[shard]
		wg.Add(1)
		run := func() error {
			defer wg.Done()
			var (
				deleted, returned bool
				err               error
			)
			// from a defer, so a delete that panics drops its shard too rather than leaving
			// the next round to dispatch it into the same panic
			defer func() { record(shard, deleted, err, returned) }()
			deleted, err = deleteShard(shard, uuids)
			returned = true
			return nil
		}

		// RunRecovered contains a panic to this shard rather than unwinding the sweep, and records
		// it on the group for WaitAndCollect. The error it returns is that same panic, as run
		// itself returns only nil.
		if idx == len(shards)-1 || !eg.TryGo(run, class, shard) {
			_ = eg.RunRecovered(run, class, shard)
		}

		if ctx.Err() != nil {
			return anyDeleted.Load(), true
		}
	}
	wg.Wait()

	return anyDeleted.Load(), false
}

func (i *Index) IncomingDeleteObjectsExpired(ctx context.Context, eg *enterrors.ErrorGroupWrapper, ec errorcompounder.ErrorCompounder,
	deleteOnPropName string, ttlThreshold, deletionTime time.Time, countDeleted func(int32), schemaVersion uint64,
) {
	// use closing context to stop long-running TTL deletions in case index is closed
	mergedCtx, _ := mergeContexts(ctx, i.closingCtx, i.logger)
	i.incomingDeleteObjectsExpired(mergedCtx, eg, ec, deleteOnPropName, ttlThreshold, deletionTime, countDeleted, schemaVersion)
}

func (i *Index) incomingDeleteObjectsExpired(ctx context.Context, eg *enterrors.ErrorGroupWrapper, ec errorcompounder.ErrorCompounder,
	deleteOnPropName string, ttlThreshold, deletionTime time.Time, countDeleted func(int32), schemaVersion uint64,
) {
	class := i.getClass()
	if err := context.Cause(ctx); err != nil {
		ec.AddGroups(err, class.Class)
		return
	}

	filter := &filters.LocalFilter{Root: &filters.Clause{
		Operator: filters.OperatorLessThanEqual,
		Value: &filters.Value{
			Value: ttlThreshold,
			Type:  schema.DataTypeDate,
		},
		On: &filters.Path{
			Class:    schema.ClassName(class.Class),
			Property: schema.PropertyName(deleteOnPropName),
		},
	}}

	// the replication properties determine how aggressive the errors are returned and does not change anything about
	// the server's behaviour. Therefore, we set it to QUORUM to be able to log errors in case the deletion does not
	// succeed on too many nodes. In the case of errors a node might retain the object past its TTL. However, when the
	// deletion process happens to run on that node again, the object will be deleted then.
	replProps := defaultConsistency()

	if multitenancy.IsMultiTenant(class.MultiTenancyConfig) {
		tenants, err := i.schemaReader.Shards(class.Class)
		if err != nil {
			ec.AddGroups(fmt.Errorf("get tenants: %w", err), class.Class)
			return
		}

		autoActivationEnabled := schema.AutoTenantActivationEnabled(class)

		for _, tenant := range tenants {
			// Don't force-load an idle tenant on every sweep; it's cleaned once it materializes.
			if i.shardIsLazyUnloaded(tenant) {
				continue
			}
			eg.Go(func() error {
				// processedBatches is intentionally shared between the findUUIDs and processBatch
				// closures below — both run within this single goroutine, so there is no race.
				processedBatches := 0
				pauseLogger := i.logger.WithFields(logrus.Fields{
					"action":     "objects_ttl_deletion",
					"collection": class.Class,
					"shard":      tenant,
				})

				loop := tenantTTLLoop{
					class:                 class.Class,
					tenant:                tenant,
					autoActivationEnabled: autoActivationEnabled,
					mgr:                   i.tenantsManager,
					findUUIDs: func(ctx context.Context) ([]strfmt.UUID, error) {
						perShardLimit := i.Config.ObjectsTTLBatchSize.Get()
						tenants2uuids, err := i.findUUIDsForExpiredObjects(ctx, filter, tenant, replProps, perShardLimit)
						if err != nil {
							return nil, err
						}
						return tenants2uuids[tenant], nil
					},
					processBatch: func(ctx context.Context, uuids []strfmt.UUID) (bool, error) {
						n, batchErr := i.incomingDeleteObjectsExpiredUuids(ctx, deletionTime, "", tenant,
							uuids, replProps, schemaVersion)
						countDeleted(n)
						if batchErr != nil {
							batchErr = fmt.Errorf("batch delete: %w", batchErr)
						}
						if ttlBatchFailedOutright(n, batchErr) {
							return false, batchErr
						}
						if err := ttlPauseAfterBatch(ctx, &processedBatches,
							i.Config.ObjectsTTLPauseEveryNoBatches.Get(),
							i.Config.ObjectsTTLPauseDuration.Get(), pauseLogger); err != nil {
							return n > 0, ttlBatchOutcome(batchErr, err)
						}
						return n > 0, batchErr
					},
				}
				loop.run(ctx, ec)
				return nil
			}, class.Class, tenant)
			if ctx.Err() != nil {
				break
			}
		}
		return
	}

	eg.Go(func() error {
		processedBatches := 0
		pauseLogger := i.logger.WithFields(logrus.Fields{
			"action":     "objects_ttl_deletion",
			"collection": class.Class,
		})
		dropped := map[string]struct{}{}
		filed := map[string]struct{}{}
		considered := map[string]struct{}{}
		rounds := 0
		sweepStopped := false

		// one line per class, not per shard: an abandoned shard keeps its expired objects until the
		// next sweep, and N entries inside one compounded error string cannot be read as a count
		defer func() {
			if sweepStopped {
				// a shard handed to the group may still be recording into dropped and filed
				return
			}
			i.logger.WithFields(logrus.Fields{
				"action":     "objects_ttl_deletion",
				"collection": class.Class,
				"rounds":     rounds,
				"shards":     len(considered),
				"abandoned":  len(dropped),
				"filed":      len(filed),
			}).Debug("shard sweep finished")
		}()
		deleteShard := func(shard string, uuids []strfmt.UUID) (bool, error) {
			n, err := i.incomingDeleteObjectsExpiredUuids(ctx, deletionTime, shard, "",
				uuids, replProps, schemaVersion)
			countDeleted(n)
			if err != nil {
				return n > 0, fmt.Errorf("batch delete: %w", err)
			}
			return n > 0, nil
		}

		// find uuids up to limit -> delete -> find uuids up to limit -> delete -> ... until no uuids left
		for {
			if err := context.Cause(ctx); err != nil {
				ec.AddGroups(err, class.Class)
				return nil
			}

			rounds++
			perShardLimit := i.Config.ObjectsTTLBatchSize.Get()
			shards2uuids, err := i.findUUIDsForExpiredObjects(ctx, filter, "", replProps, perShardLimit)
			if err != nil {
				ec.AddGroups(fmt.Errorf("find uuids: %w", err), class.Class)
				return nil
			}
			for shard := range shards2uuids {
				considered[shard] = struct{}{}
			}

			deleted, stopped := deleteFromShards(ctx, eg, ec, class.Class, shards2uuids,
				dropped, filed, deleteShard)
			if stopped || !deleted {
				sweepStopped = stopped
				return nil
			}

			if err := ttlPauseAfterBatch(ctx, &processedBatches,
				i.Config.ObjectsTTLPauseEveryNoBatches.Get(),
				i.Config.ObjectsTTLPauseDuration.Get(), pauseLogger); err != nil {
				ec.AddGroups(err, class.Class)
				return nil
			}
		}
	}, class.Class)
}

func (i *Index) incomingDeleteObjectsExpiredUuids(ctx context.Context,
	deletionTime time.Time, shard, tenant string, uuids []strfmt.UUID,
	replProps *additional.ReplicationProperties, schemaVersion uint64,
) (deleted int32, err error) {
	i.metrics.IncObjectsTtlBatchDeletesCount()
	i.metrics.IncObjectsTtlBatchDeletesRunning()

	started := time.Now()
	inputKey := shard
	if tenant != "" {
		inputKey = tenant
	}

	logger := i.logger.WithFields(logrus.Fields{
		"action":     "objects_ttl_deletion",
		"collection": i.Config.ClassName.String(),
		"shard":      inputKey,
	})
	logger.WithFields(logrus.Fields{
		"size": len(uuids),
	}).Debug("batch delete started")

	defer func() {
		took := time.Since(started)

		i.metrics.DecObjectsTtlBatchDeletesRunning()
		i.metrics.ObserveObjectsTtlBatchDeletesDuration(took)
		i.metrics.AddObjectsTtlBatchDeletesObjectsDeleted(float64(deleted))

		logger := logger.WithFields(logrus.Fields{
			"took":    took.String(),
			"deleted": deleted,
			"failed":  int32(len(uuids)) - deleted,
		})
		if err != nil {
			i.metrics.IncObjectsTtlBatchDeletesFailureCount()

			// logs as debug, combined error is logged as error anyway
			logger.WithError(err).Debug("batch delete failed")
			return
		}
		logger.Debug("batch delete finished")
	}()

	input := map[string][]strfmt.UUID{inputKey: uuids}
	resp, err := i.batchDeleteObjects(ctx, input, deletionTime, false, replProps, schemaVersion, tenant)
	if err != nil {
		return deleted, err
	}

	ec := errorcompounder.New()
	for idx := range resp {
		if err := resp[idx].Err; err != nil {
			ec.Add(fmt.Errorf("%s: %w", resp[idx].UUID, err))
			continue
		}
		deleted++
	}

	return deleted, ec.ToErrorLimited(3)
}

func (i *Index) findUUIDsForExpiredObjects(ctx context.Context,
	filters *filters.LocalFilter, tenant string, repl *additional.ReplicationProperties,
	perShardLimit int,
) (shards2uuids map[string][]strfmt.UUID, err error) {
	i.metrics.IncObjectsTtlFindUuidsCount()
	i.metrics.IncObjectsTtlFindUuidsRunning()

	started := time.Now()

	logger := i.logger.WithFields(logrus.Fields{
		"action":     "objects_ttl_deletion",
		"collection": i.Config.ClassName.String(),
	})
	logger.Debug("find uuids started")

	defer func() {
		took := time.Since(started)
		found := 0
		for _, uuids := range shards2uuids {
			found += len(uuids)
		}

		i.metrics.DecObjectsTtlFindUuidsRunning()
		i.metrics.ObserveObjectsTtlFindUuidsDuration(took)
		i.metrics.AddObjectsTtlFindUuidsObjectsFound(float64(found))

		logger := logger.WithFields(logrus.Fields{
			"took":  took.String(),
			"found": found,
		})
		if err != nil {
			i.metrics.IncObjectsTtlFindUuidsFailureCount()

			// logs as debug, combined error is logged as error anyway
			logger.WithError(err).Debug("find uuids failed")
			return
		}
		logger.Debug("find uuids finished")
	}()

	return i.findUUIDs(ctx, filters, tenant, repl, perShardLimit)
}

// run executes the TTL deletion loop for this tenant.
func (l *tenantTTLLoop) run(ctx context.Context, ec errorcompounder.ErrorCompounder) {
	deactivate := false
	activityChecked := false

	defer l.ensureDeactivation(ec, &deactivate)

	for {
		if err := context.Cause(ctx); err != nil {
			ec.AddGroups(err, l.class, l.tenant)
			return
		}

		if l.autoActivationEnabled && !activityChecked {
			shouldDeactivate, err := l.checkActivity()
			if err != nil {
				ec.AddGroups(err, l.class, l.tenant)
				return
			}
			deactivate = shouldDeactivate
			activityChecked = true
		}

		if l.findAndDelete(ctx, ec, &deactivate) {
			return
		}
	}
}

// checkActivity queries the tenant's current activity status.
// Returns true when the tenant is COLD and should be re-deactivated after TTL processing.
func (l *tenantTTLLoop) checkActivity() (shouldDeactivate bool, err error) {
	tenants2status, err := l.mgr.TenantsStatus(l.class, l.tenant)
	if err != nil {
		return false, fmt.Errorf("check activity status: %w", err)
	}
	return tenants2status[l.tenant] == models.TenantActivityStatusCOLD, nil
}

// findAndDelete fetches the next batch of expired UUIDs, deletes them, and returns true where the
// loop should stop. A batch that deleted part of its uuids and failed on the rest made progress, so
// the loop goes on and files the failure once for the sweep.
func (l *tenantTTLLoop) findAndDelete(ctx context.Context, ec errorcompounder.ErrorCompounder, deactivate *bool) (done bool) {
	uuids, err := l.findUUIDs(ctx)
	if err != nil {
		if errors.Is(err, enterrors.ErrTenantNotActive) {
			// The tenant was never successfully activated — no RAFT deactivation needed.
			*deactivate = false
		} else {
			ec.AddGroups(fmt.Errorf("find uuids: %w", err), l.class, l.tenant)
		}
		return true
	}

	if len(uuids) == 0 {
		return true
	}

	deleted, err := l.processBatch(ctx, uuids)
	reason := ttlFailureReason(ctx, deleted, err)
	if reason != nil && !l.filed {
		ec.AddGroups(reason, l.class, l.tenant)
		l.filed = true
	}
	return !deleted
}

// ensureDeactivation re-deactivates the tenant if it was auto-activated for TTL processing.
// Uses a bounded timeout so a stuck RAFT call cannot block the goroutine indefinitely.
func (l *tenantTTLLoop) ensureDeactivation(ec errorcompounder.ErrorCompounder, deactivate *bool) {
	if !*deactivate {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), ttlDeactivateTimeout)
	defer cancel()
	if err := l.mgr.DeactivateTenants(ctx, l.class, l.tenant); err != nil {
		ec.AddGroups(fmt.Errorf("deactivate tenant: %w", err), l.class, l.tenant)
	}
}

// ttlBatchFailedOutright reports a batch that deleted none of its uuids and failed. Counting only
// batches that failed on nothing would let a tenant holding one undeletable object run every batch
// of its sweep back to back.
func ttlBatchFailedOutright(deleted int32, err error) bool {
	return err != nil && deleted == 0
}

// ttlBatchOutcome reports what a batch that was followed by a stopped pause should file. A sweep
// stopped while it slept must not replace the failure the batch itself reported.
func ttlBatchOutcome(batchErr, pauseErr error) error {
	switch {
	case batchErr == nil:
		return pauseErr
	case pauseErr == nil:
		return batchErr
	default:
		return fmt.Errorf("%w; %w", batchErr, pauseErr)
	}
}

// ttlPauseAfterBatch counts one finished unit of work, sleeps once the count reaches the configured
// number so a sweep cannot starve foreground traffic, and reports the cause of a sweep stopped while
// it slept. A unit is one batch in the tenant arm, one round of shard batches in the shard arm.
func ttlPauseAfterBatch(ctx context.Context, processed *int, every int, dur time.Duration,
	logger logrus.FieldLogger,
) error {
	*processed++
	if dur <= 0 || every <= 0 || *processed < every {
		return nil
	}

	started := time.Now()
	ended, err := sleepWithCtx(ctx, dur)
	if err != nil {
		return err
	}
	logger.Debugf("paused for %s after processing %d batches", ended.Sub(started), *processed)
	*processed = 0
	return nil
}

func sleepWithCtx(ctx context.Context, d time.Duration) (val time.Time, err error) {
	timer := time.NewTimer(d)
	select {
	case t := <-timer.C:
		return t, nil
	case <-ctx.Done():
		timer.Stop()
		return time.Time{}, context.Cause(ctx)
	}
}

// TODO aliszka:ttl find better way to merge contexts
func mergeContexts(parentCtx, secondCtx context.Context, logger logrus.FieldLogger) (context.Context, context.CancelCauseFunc) {
	ctx, cancel := context.WithCancelCause(parentCtx)

	enterrors.GoWrapper(func() {
		select {
		case <-secondCtx.Done():
			cancel(context.Cause(secondCtx))
		case <-parentCtx.Done():
		}
	}, logger)

	return ctx, cancel
}
