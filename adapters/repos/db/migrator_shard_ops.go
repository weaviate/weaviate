//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/types"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
)

func (m *Migrator) hot(ctx context.Context, idx *Index, hot []string, ec *errorcompounder.ErrorCompounder) {
	for _, name := range hot {
		shard, err := idx.getOrInitLocalShard(ctx, name)
		ec.Add(err)
		if err != nil {
			continue
		}

		// if the shard is a lazy load shard, we need to force its activation now
		asLL, ok := shard.(*LazyLoadShard)
		if !ok {
			continue
		}

		name := name // prevent loop variable capture
		enterrors.GoWrapper(func() {
			// The timeout is rather arbitrary. It's meant to be so high that it can
			// never stop a valid tenant activation use case, but low enough to
			// prevent a context-leak.
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Hour)
			defer cancel()

			if err := asLL.Load(ctx); err != nil {
				idx.logger.WithFields(logrus.Fields{
					"action": "tenant_activation_lazy_laod_shard",
					"shard":  name,
				}).WithError(err).Errorf("loading shard %q failed", name)
			}
		}, idx.logger)
	}
}

func (m *Migrator) cold(ctx context.Context, idx *Index, cold []string, ec *errorcompounder.ErrorCompounder) {
	idx.backupMutex.RLock()
	defer idx.backupMutex.RUnlock()

	eg := enterrors.NewErrorGroupWrapper(m.logger)
	eg.SetLimit(_NUMCPU * 2)

	for _, name := range cold {
		name := name
		eg.Go(func() error {
			shard := func() ShardLike {
				idx.shardInUseLocks.Lock(name)
				defer idx.shardInUseLocks.Unlock(name)

				return idx.shards.Load(name)
			}()

			if shard == nil {
				return nil // shard already does not exist or inactive
			}

			idx.shardCreateLocks.Lock(name)
			defer idx.shardCreateLocks.Unlock(name)

			idx.shards.LoadAndDelete(name)

			if err := shard.Shutdown(ctx); err != nil {
				if !errors.Is(err, errAlreadyShutdown) {
					ec.Add(err)
					idx.logger.WithField("action", "shutdown_shard").
						WithField("shard", shard.ID()).Error(err)
				}
				m.logger.WithField("shard", shard.Name()).Debug("was already shut or dropped")
			}
			return nil
		})
	}
	eg.Wait()
}

func (m *Migrator) frozen(idx *Index, frozen []string, ec *errorcompounder.ErrorCompounder) {
	idx.backupMutex.RLock()
	defer idx.backupMutex.RUnlock()

	eg := enterrors.NewErrorGroupWrapper(m.logger)
	eg.SetLimit(_NUMCPU * 2)

	for _, name := range frozen {
		name := name
		eg.Go(func() error {
			shard := func() ShardLike {
				idx.shardInUseLocks.Lock(name)
				defer idx.shardInUseLocks.Unlock(name)

				return idx.shards.Load(name)
			}()

			if shard == nil {
				// shard already does not exist or inactive, so remove local files if exists
				// this pass will happen if the shard was COLD for example
				if err := os.RemoveAll(fmt.Sprintf("%s/%s", idx.path(), name)); err != nil {
					ec.Add(err)
					return fmt.Errorf("attempt to delete local fs for shard %s: %w", name, err)
				}
				return nil
			}

			idx.shardCreateLocks.Lock(name)
			defer idx.shardCreateLocks.Unlock(name)

			idx.shards.LoadAndDelete(name)

			if err := shard.drop(); err != nil {
				ec.Add(err)
			}
			return nil
		})
	}
	eg.Wait()
}

func (m *Migrator) freeze(ctx context.Context, idx *Index, class string, freeze []string, ec *errorcompounder.ErrorCompounder) {
	if m.cloud == nil {
		ec.Add(fmt.Errorf("offload to cloud module is not enabled"))
		return
	}

	idx.backupMutex.RLock()
	defer idx.backupMutex.RUnlock()

	eg := enterrors.NewErrorGroupWrapper(m.logger)
	eg.SetLimit(_NUMCPU * 2)

	for _, name := range freeze {
		name := name
		originalStatus := models.TenantActivityStatusHOT
		eg.Go(func() error {
			shard := func() ShardLike {
				idx.shardInUseLocks.Lock(name)
				defer idx.shardInUseLocks.Unlock(name)

				return idx.shards.Load(name)
			}()

			if shard == nil {
				// shard already does not exist or inactive
				originalStatus = models.TenantActivityStatusCOLD
			}

			idx.shardCreateLocks.Lock(name)
			defer idx.shardCreateLocks.Unlock(name)

			if shard != nil {
				// if err := shard.UpdateStatus(storagestate.StatusReadOnly.String()); err != nil {
				// 	ec.Add(err)
				// 	return fmt.Errorf("attempt to mark read-only: %w", err)
				// }
				if err := shard.BeginBackup(ctx); err != nil {
					ec.Add(err)
					return fmt.Errorf("attempt to mark begin offloading: %w", err)
				}
			}

			enterrors.GoWrapper(func() {
				cmd := command.TenantsProcessRequest{Node: m.nodeId}
				err := m.cloud.Upload(ctx, class, name, m.nodeId)
				if err != nil {
					m.logger.Error(err)
					cmd.TenantsProcess = []*command.TenantsProcess{
						{
							Tenant: &command.Tenant{
								Name:   name,
								Status: originalStatus,
							},
							Op: command.TenantsProcess_OP_ABORT,
						},
					}
				} else {
					cmd.TenantsProcess = []*command.TenantsProcess{
						{
							Tenant: &command.Tenant{
								Name:   name,
								Status: models.TenantActivityStatusFROZEN,
							},
							Op: command.TenantsProcess_OP_DONE,
						},
					}
				}

				if _, err := m.cluster.UpdateTenantsProcess(class, &cmd); err != nil {
					m.logger.Error(err)
				}

			}, idx.logger)

			return nil
		})
	}
	eg.Wait()
}

func (m *Migrator) unfreeze(ctx context.Context, idx *Index, class string, unfreeze []string, ec *errorcompounder.ErrorCompounder) {
	if m.cloud == nil {
		ec.Add(fmt.Errorf("offload to cloud module is not enabled"))
		return
	}
	idx.backupMutex.RLock()
	defer idx.backupMutex.RUnlock()

	eg := enterrors.NewErrorGroupWrapper(m.logger)
	eg.SetLimit(_NUMCPU * 2)

	for _, name := range unfreeze {
		split := strings.Split(name, "-")
		name := split[0]
		nodeName := split[1]
		// originalStatus := models.TenantActivityStatusHOT
		eg.Go(func() error {
			// shard := func() ShardLike {
			// 	idx.shardInUseLocks.Lock(name)
			// 	defer idx.shardInUseLocks.Unlock(name)

			// 	return idx.shards.Load(name)
			// }()

			// if shard == nil {
			// 	// shard already does not exist or inactive
			// 	originalStatus = models.TenantActivityStatusCOLD
			// }

			idx.shardCreateLocks.Lock(name)
			defer idx.shardCreateLocks.Unlock(name)

			// if shard != nil {
			// 	// if err := shard.UpdateStatus(storagestate.StatusReadOnly.String()); err != nil {
			// 	// 	ec.Add(err)
			// 	// 	return fmt.Errorf("attempt to mark read-only: %w", err)
			// 	// }
			// 	if err := shard.BeginBackup(ctx); err != nil {
			// 		ec.Add(err)
			// 		return fmt.Errorf("attempt to mark begin offloading: %w", err)
			// 	}
			// }

			enterrors.GoWrapper(func() {
				cmd := command.TenantsProcessRequest{Node: m.nodeId}
				err := m.cloud.Download(ctx, class, name, nodeName)
				if err != nil {
					m.logger.Error(err)
					// cmd.TenantsProcess = []*command.TenantsProcess{
					// 	{
					// 		Tenant: &command.Tenant{
					// 			Name:   name,
					// 			Status: models.TenantActivityStatusFROZEN,
					// 		},
					// 		Op: command.TenantsProcess_OP_ABORT,
					// 	},
					// }
				} else {
					cmd.TenantsProcess = []*command.TenantsProcess{
						{
							Tenant: &command.Tenant{
								Name:   name,
								Status: types.TenantActivityStatusUNFROZEN,
							},
							Op: command.TenantsProcess_OP_DONE,
						},
					}
				}

				if _, err := m.cluster.UpdateTenantsProcess(class, &cmd); err != nil {
					m.logger.Error(err)
				}

			}, idx.logger)

			return nil
		})
	}
	eg.Wait()
}
