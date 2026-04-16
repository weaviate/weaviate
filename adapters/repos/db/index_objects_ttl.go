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
	processBatch          func(ctx context.Context, uuids []strfmt.UUID) error
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
			eg.Go(func() error {
				// processedBatches is intentionally shared between the findUUIDs and processBatch
				// closures below — both run within this single goroutine, so there is no race.
				processedBatches := 0

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
					processBatch: func(ctx context.Context, uuids []strfmt.UUID) error {
						if err := i.incomingDeleteObjectsExpiredUuids(ctx, deletionTime, "", tenant,
							uuids, countDeleted, replProps, schemaVersion); err != nil {
							ec.AddGroups(fmt.Errorf("batch delete: %w", err), class.Class, tenant)
						}
						processedBatches++
						pauseEvery := i.Config.ObjectsTTLPauseEveryNoBatches.Get()
						pauseDur := i.Config.ObjectsTTLPauseDuration.Get()
						if pauseDur > 0 && pauseEvery > 0 && processedBatches >= pauseEvery {
							t1 := time.Now()
							t2, sleepErr := sleepWithCtx(ctx, pauseDur)
							if sleepErr != nil {
								return sleepErr // caller adds to ec and stops the loop
							}
							i.logger.WithFields(logrus.Fields{
								"action":     "objects_ttl_deletion",
								"collection": class.Class,
								"shard":      tenant,
							}).Debugf("paused for %s after processing %d batches", t2.Sub(t1), processedBatches)
							processedBatches = 0
						}
						return nil
					},
				}
				loop.run(ctx, ec)
				return nil
			})
			if ctx.Err() != nil {
				break
			}
		}
		return
	}

	eg.Go(func() error {
		processedBatches := 0
		// find uuids up to limit -> delete -> find uuids up to limit -> delete -> ... until no uuids left
		for {
			if err := context.Cause(ctx); err != nil {
				ec.AddGroups(err, class.Class)
				return nil
			}

			perShardLimit := i.Config.ObjectsTTLBatchSize.Get()
			shards2uuids, err := i.findUUIDsForExpiredObjects(ctx, filter, "", replProps, perShardLimit)
			if err != nil {
				ec.AddGroups(fmt.Errorf("find uuids: %w", err), class.Class)
				return nil
			}

			shardIdx := len(shards2uuids) - 1
			anyUuidsFetched := false
			wg := new(sync.WaitGroup)
			f := func(shard string, uuids []strfmt.UUID) {
				defer wg.Done()
				if err := i.incomingDeleteObjectsExpiredUuids(ctx, deletionTime, shard, "",
					uuids, countDeleted, replProps, schemaVersion); err != nil {
					ec.AddGroups(fmt.Errorf("batch delete: %w", err), class.Class, shard)
				}
			}

			for shard, uuids := range shards2uuids {
				shardIdx--

				if len(uuids) == 0 {
					continue
				}

				anyUuidsFetched = true
				wg.Add(1)
				isLastShard := shardIdx == 0
				// if possible run in separate routine, if not run in current one
				// always run last in current one (not to start other routine,
				// while current one have to wait for the results anyway)
				if isLastShard || !eg.TryGo(func() error {
					f(shard, uuids)
					return nil
				}) {
					f(shard, uuids)
				}

				if ctx.Err() != nil {
					return nil
				}
			}
			wg.Wait()

			if !anyUuidsFetched {
				return nil
			}

			processedBatches++
			pauseEveryNoBatches := i.Config.ObjectsTTLPauseEveryNoBatches.Get()
			pauseDuration := i.Config.ObjectsTTLPauseDuration.Get()
			if pauseDuration > 0 && pauseEveryNoBatches > 0 && processedBatches >= pauseEveryNoBatches {
				t1 := time.Now()
				t2, err := sleepWithCtx(ctx, pauseDuration)
				if err != nil {
					ec.AddGroups(err, class.Class)
					return nil
				}
				i.logger.WithFields(logrus.Fields{
					"action":     "objects_ttl_deletion",
					"collection": class.Class,
				}).Debugf("paused for %s after processing %d batches", t2.Sub(t1), processedBatches)
				processedBatches = 0
			}
		}
	})
}

func (i *Index) incomingDeleteObjectsExpiredUuids(ctx context.Context,
	deletionTime time.Time, shard, tenant string, uuids []strfmt.UUID, countDeleted func(int32),
	replProps *additional.ReplicationProperties, schemaVersion uint64,
) (err error) {
	i.metrics.IncObjectsTtlBatchDeletesCount()
	i.metrics.IncObjectsTtlBatchDeletesRunning()

	started := time.Now()
	deleted := int32(0)
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
		return err
	}

	ec := errorcompounder.New()
	for idx := range resp {
		if err := resp[idx].Err; err != nil {
			ec.Add(fmt.Errorf("%s: %w", resp[idx].UUID, err))
			continue
		}
		deleted++
	}
	countDeleted(deleted)

	return ec.ToErrorLimited(3)
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

// findAndDelete fetches the next batch of expired UUIDs and deletes them.
// Returns true when the loop should stop (no more work, or an error occurred).
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

	if err := l.processBatch(ctx, uuids); err != nil {
		ec.AddGroups(err, l.class, l.tenant)
		return true
	}
	return false
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
