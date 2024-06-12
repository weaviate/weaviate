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

	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/types"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
)

func (m *Migrator) frozen(idx *Index, frozen []string, ec *errorcompounder.ErrorCompounder) {
	if m.cluster == nil {
		ec.Add(fmt.Errorf("no cluster exists in the migrator"))
		return
	}

	idx.shardTransferMutex.RLock()
	defer idx.shardTransferMutex.RUnlock()

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
					err = fmt.Errorf("attempt to delete local fs for shard %s: %w", name, err)
					ec.Add(err)
					return err
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

	if m.cluster == nil {
		ec.Add(fmt.Errorf("no cluster exists in the migrator"))
		return
	}

	idx.shardTransferMutex.RLock()
	defer idx.shardTransferMutex.RUnlock()

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
				if err := shard.HaltForTransfer(ctx); err != nil {
					ec.Add(err)
					return fmt.Errorf("attempt to mark begin offloading: %w", err)
				}
			}

			enterrors.GoWrapper(func() {
				cmd := command.TenantProcessRequest{
					Node:   m.nodeId,
					Action: command.TenantProcessRequest_ACTION_FREEZING,
				}
				err := m.cloud.Upload(ctx, class, name, m.nodeId)
				if err != nil {
					ec.Add(fmt.Errorf("uploading error: %w", err))
					cmd.Process = &command.TenantsProcess{
						Tenant: &command.Tenant{
							Name:   name,
							Status: originalStatus,
						},
						Op: command.TenantsProcess_OP_ABORT,
					}
				} else {
					cmd.Process = &command.TenantsProcess{
						Tenant: &command.Tenant{
							Name:   name,
							Status: models.TenantActivityStatusFROZEN,
						},
						Op: command.TenantsProcess_OP_DONE,
					}
				}

				if _, err := m.cluster.UpdateTenantsProcess(class, &cmd); err != nil {
					ec.Add(fmt.Errorf("UpdateTenantsProcess error: %w", err))
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

	if m.cluster == nil {
		ec.Add(fmt.Errorf("no cluster exists in the migrator"))
		return
	}

	idx.shardTransferMutex.RLock()
	defer idx.shardTransferMutex.RUnlock()

	eg := enterrors.NewErrorGroupWrapper(m.logger)
	eg.SetLimit(_NUMCPU * 2)

	for _, name := range unfreeze {
		split := strings.Split(name, "#")
		if len(split) < 2 {
			ec.Add(fmt.Errorf("can't detect the old node name"))
			continue
		}
		name := split[0]
		nodeName := split[1]
		eg.Go(func() error {
			idx.shardCreateLocks.Lock(name)
			defer idx.shardCreateLocks.Unlock(name)

			enterrors.GoWrapper(func() {
				cmd := command.TenantProcessRequest{
					Node:   m.nodeId,
					Action: command.TenantProcessRequest_ACTION_UNFREEZING,
				}
				err := m.cloud.Download(ctx, class, name, nodeName)
				if err != nil {
					ec.Add(fmt.Errorf("downloading error: %w", err))
					// one success will be sufficient for changing the status
					cmd.Process = &command.TenantsProcess{
						Tenant: &command.Tenant{
							Name:   name,
							Status: types.TenantActivityStatusUNFROZEN,
						},
						Op: command.TenantsProcess_OP_ABORT,
					}
				} else {
					if err := m.cloud.Delete(ctx, class, name, nodeName); err != nil {
						// we just logging in case of we are not able to delete the cloud
						m.logger.Error("deleting error: %w", err)
					}
					cmd.Process = &command.TenantsProcess{
						Tenant: &command.Tenant{
							Name:   name,
							Status: types.TenantActivityStatusUNFROZEN,
						},
						Op: command.TenantsProcess_OP_DONE,
					}
				}

				if _, err := m.cluster.UpdateTenantsProcess(class, &cmd); err != nil {
					ec.Add(fmt.Errorf("UpdateTenantsProcess error: %w", err))
				}
			}, idx.logger)

			return nil
		})
	}
	eg.Wait()
}
