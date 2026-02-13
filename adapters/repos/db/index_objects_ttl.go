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

func (i *Index) IncomingDeleteObjectsExpired(eg *enterrors.ErrorGroupWrapper, ec errorcompounder.ErrorCompounder,
	deleteOnPropName string, ttlThreshold, deletionTime time.Time, countDeleted func(int32), schemaVersion uint64,
) {
	// use closing context to stop long-running TTL deletions in case index is closed
	i.incomingDeleteObjectsExpired(i.closingCtx, eg, ec, deleteOnPropName, ttlThreshold, deletionTime, countDeleted, schemaVersion)
}

func (i *Index) incomingDeleteObjectsExpired(ctx context.Context, eg *enterrors.ErrorGroupWrapper, ec errorcompounder.ErrorCompounder,
	deleteOnPropName string, ttlThreshold, deletionTime time.Time, countDeleted func(int32), schemaVersion uint64,
) {
	class := i.getClass()
	if err := ctx.Err(); err != nil {
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

	sleepWithCtx := func(ctx context.Context, d time.Duration) (val time.Time, err error) {
		timer := time.NewTimer(d)
		select {
		case t := <-timer.C:
			return t, nil
		case <-ctx.Done():
			timer.Stop()
			return time.Time{}, ctx.Err()
		}
	}

	if multitenancy.IsMultiTenant(class.MultiTenancyConfig) {
		tenants, err := i.schemaReader.Shards(class.Class)
		if err != nil {
			ec.AddGroups(fmt.Errorf("get tenants: %w", err), class.Class)
			return
		}

		autoActivationEnabled := schema.AutoTenantActivationEnabled(class)

		for _, tenant := range tenants {
			eg.Go(func() error {
				deactivate := false
				activityChecked := false
				processedBatches := 0
				// find uuids up to limit -> delete -> find uuids up to limit -> delete -> ... until no uuids left
				for {
					if err := ctx.Err(); err != nil {
						ec.AddGroups(err, class.Class, tenant)
						return nil
					}

					// if autoActivationEnabled, findUUIDsForExpiredObjects/findUUIDs call will implicitly activate tenant.
					// activated tenant should be deactivated back after finished deletions
					if autoActivationEnabled && !activityChecked {
						tenants2status, err := i.tenantsManager.TenantsStatus(class.Class, tenant)
						if err != nil {
							ec.AddGroups(fmt.Errorf("check activity status: %w", err), class.Class, tenant)
							return nil
						}
						deactivate = tenants2status[tenant] == models.TenantActivityStatusCOLD
						activityChecked = true
					}

					perShardLimit := i.Config.ObjectsTTLBatchSize.Get()
					tenants2uuids, err := i.findUUIDsForExpiredObjects(ctx, filter, tenant, replProps, perShardLimit)
					if err != nil {
						// skip inactive tenants
						if !errors.Is(err, enterrors.ErrTenantNotActive) {
							ec.AddGroups(fmt.Errorf("find uuids: %w", err), class.Class, tenant)
						}
						return nil
					}

					uuids := tenants2uuids[tenant]
					if len(uuids) == 0 {
						if deactivate {
							if err := i.tenantsManager.DeactivateTenants(ctx, class.Class, tenant); err != nil {
								ec.AddGroups(fmt.Errorf("deactivate tenant: %w", err), class.Class, tenant)
							}
						}
						return nil
					}

					if err := i.incomingDeleteObjectsExpiredUuids(ctx, deletionTime, "", tenant,
						uuids, countDeleted, replProps, schemaVersion); err != nil {
						ec.AddGroups(fmt.Errorf("batch delete: %w", err), class.Class, tenant)
					}
					processedBatches++

					pauseEveryNoBatches := i.Config.ObjectsTTLPauseEveryNoBatches.Get()
					pauseDuration := i.Config.ObjectsTTLPauseDuration.Get()
					if pauseDuration > 0 && pauseEveryNoBatches > 0 && processedBatches >= pauseEveryNoBatches {
						t1 := time.Now()
						t2, err := sleepWithCtx(ctx, pauseDuration)
						if err != nil {
							ec.AddGroups(ctx.Err(), class.Class, tenant)
							return nil
						}
						i.logger.WithFields(logrus.Fields{
							"action":     "objects_ttl_deletion",
							"collection": class.Class,
							"shard":      tenant,
						}).Debugf("paused for %s after processing %d batches", t2.Sub(t1), processedBatches)
						processedBatches = 0
					}
				}
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
			if err := ctx.Err(); err != nil {
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
					ec.AddGroups(ctx.Err(), class.Class)
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

	maxErrors := 3
	errsCount := 0
	ecBatch := errorcompounder.New()
	for idx := range resp {
		if err := resp[idx].Err; err != nil {
			// limit number of returned errors to [maxErrors]
			if errsCount < maxErrors {
				errsCount++
				ecBatch.Add(fmt.Errorf("%s: %w", resp[idx].UUID, err))
			}
		} else {
			deleted++
		}
	}
	countDeleted(deleted)

	return ecBatch.ToError()
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
