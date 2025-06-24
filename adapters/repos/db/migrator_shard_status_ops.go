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
	"sync"

	"github.com/sirupsen/logrus"

	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
)

func (m *Migrator) frozen(ctx context.Context, idx *Index, frozen []string, ec *errorcompounder.SafeErrorCompounder) {
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
			shard, release, err := idx.getOrInitShard(ctx, name)
			if err != nil {
				ec.Add(err)
				return nil
			}

			defer release()

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

func (m *Migrator) freeze(ctx context.Context, idx *Index, class string, freeze []string, ec *errorcompounder.SafeErrorCompounder) {
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

	cmd := command.TenantProcessRequest{
		Node:             m.nodeId,
		Action:           command.TenantProcessRequest_ACTION_FREEZING,
		TenantsProcesses: make([]*command.TenantsProcess, len(freeze)),
	}

	for uidx, name := range freeze {
		name := name
		uidx := uidx
		eg.Go(func() error {
			originalStatus := models.TenantActivityStatusHOT
			shard, release, err := idx.getOrInitShard(ctx, name)
			if err != nil {
				m.logger.WithFields(logrus.Fields{
					"action": "get_local_shard_no_shutdown",
					"error":  err,
					"name":   class,
					"tenant": name,
				}).Error("getLocalShardNoShutdown")
				cmd.TenantsProcesses[uidx] = &command.TenantsProcess{
					Tenant: &command.Tenant{
						Name:   name,
						Status: originalStatus,
					},
					Op: command.TenantsProcess_OP_ABORT,
				}
				ec.Add(err)
				return nil
			}

			defer release()

			if shard == nil {
				// shard already does not exist or inactive
				originalStatus = models.TenantActivityStatusCOLD
			}

			idx.shardCreateLocks.Lock(name)
			defer idx.shardCreateLocks.Unlock(name)

			if shard != nil {
				if err := shard.HaltForTransfer(ctx, true, 0); err != nil {
					m.logger.WithFields(logrus.Fields{
						"action": "halt_for_transfer",
						"error":  err,
						"name":   class,
						"tenant": name,
					}).Error("HaltForTransfer")
					cmd.TenantsProcesses[uidx] = &command.TenantsProcess{
						Tenant: &command.Tenant{
							Name:   name,
							Status: originalStatus,
						},
						Op: command.TenantsProcess_OP_ABORT,
					}
					ec.Add(err)
					return fmt.Errorf("attempt to mark begin offloading: %w", err)
				}
			}

			if err := m.cloud.Upload(ctx, class, name, m.nodeId); err != nil {
				m.logger.WithFields(logrus.Fields{
					"action": "upload_tenant_to_cloud",
					"error":  err,
					"name":   class,
					"tenant": name,
				}).Error("uploading")

				ec.Add(fmt.Errorf("uploading error: %w", err))
				cmd.TenantsProcesses[uidx] = &command.TenantsProcess{
					Tenant: &command.Tenant{
						Name:   name,
						Status: originalStatus,
					},
					Op: command.TenantsProcess_OP_ABORT,
				}
			} else {
				cmd.TenantsProcesses[uidx] = &command.TenantsProcess{
					Tenant: &command.Tenant{
						Name:   name,
						Status: models.TenantActivityStatusFROZEN,
					},
					Op: command.TenantsProcess_OP_DONE,
				}
			}

			return nil
		})
	}
	eg.Wait()

	if len(cmd.TenantsProcesses) == 0 {
		m.logger.WithFields(logrus.Fields{
			"errors":  ec.ToError().Error(),
			"action":  "update_tenants_process",
			"name":    class,
			"process": cmd.TenantsProcesses,
		}).Error("empty UpdateTenantsProcess")
		return
	}

	enterrors.GoWrapper(func() {
		if _, err := m.cluster.UpdateTenantsProcess(ctx, class, &cmd); err != nil {
			m.logger.WithFields(logrus.Fields{
				"action":  "update_tenants_process",
				"error":   err,
				"name":    class,
				"process": cmd.TenantsProcesses,
			}).Error("UpdateTenantsProcess")
			ec.Add(fmt.Errorf("UpdateTenantsProcess error: %w", err))
		}
	}, idx.logger)
}

func (m *Migrator) unfreeze(ctx context.Context, idx *Index, class string, unfreeze []string, ec *errorcompounder.SafeErrorCompounder) {
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
	tenantsToBeDeletedFromCloud := sync.Map{}
	cmd := command.TenantProcessRequest{
		Node:             m.nodeId,
		Action:           command.TenantProcessRequest_ACTION_UNFREEZING,
		TenantsProcesses: make([]*command.TenantsProcess, len(unfreeze)),
	}

	for uidx, name := range unfreeze {
		name := name
		uidx := uidx
		eg.Go(func() error {
			// # is a delineator shall come from RAFT and it's away e.g. tenant1#node1
			// to identify which node path in the cloud shall we get the data from.
			// it's made because nodeID could be changed on download based on new candidates
			// when the tenant is unfrozen
			split := strings.Split(name, "#")
			if len(split) < 2 {
				cmd.TenantsProcesses[uidx] = &command.TenantsProcess{
					Tenant: &command.Tenant{
						Name: name,
					},
					Op: command.TenantsProcess_OP_ABORT,
				}
				err := fmt.Errorf("can't detect the old node name")
				ec.Add(err)
				return err
			}
			name := split[0]
			nodeID := split[1]
			idx.shardCreateLocks.Lock(name)
			defer idx.shardCreateLocks.Unlock(name)

			if err := m.cloud.Download(ctx, class, name, nodeID); err != nil {
				m.logger.WithFields(logrus.Fields{
					"action": "download_tenant_from_cloud",
					"error":  err,
					"name":   class,
					"tenant": name,
				}).Error("downloading")
				ec.Add(fmt.Errorf("downloading error: %w", err))
				// one success will be sufficient for changing the status
				// no status provided here it will be detected which status
				// requested by RAFT processes
				cmd.TenantsProcesses[uidx] = &command.TenantsProcess{
					Tenant: &command.Tenant{
						Name: name,
					},
					Op: command.TenantsProcess_OP_ABORT,
				}
			} else {
				cmd.TenantsProcesses[uidx] = &command.TenantsProcess{
					Tenant: &command.Tenant{
						Name: name,
					},
					Op: command.TenantsProcess_OP_DONE,
				}
				tenantsToBeDeletedFromCloud.Store(name, nodeID)
			}

			return nil
		})
	}
	eg.Wait()

	if len(cmd.TenantsProcesses) == 0 {
		m.logger.WithFields(logrus.Fields{
			"errors":  ec.ToError().Error(),
			"action":  "update_tenants_process",
			"name":    class,
			"process": cmd.TenantsProcesses,
		}).Error("empty UpdateTenantsProcess")
		return
	}

	enterrors.GoWrapper(func() {
		if _, err := m.cluster.UpdateTenantsProcess(ctx, class, &cmd); err != nil {
			m.logger.WithFields(logrus.Fields{
				"action":  "update_tenants_process",
				"error":   err,
				"name":    class,
				"process": cmd.TenantsProcesses,
			}).Error("UpdateTenantsProcess")
			ec.Add(fmt.Errorf("UpdateTenantsProcess error: %w", err))
			return
		}
	}, idx.logger)

	tenantsToBeDeletedFromCloud.Range(func(name, nodeID any) bool {
		m.logger.WithFields(logrus.Fields{
			"action":      "deleting_tenant_from_cloud",
			"name":        class,
			"node":        nodeID,
			"currentNode": m.nodeId,
			"tenant":      name,
		}).Debug()

		if err := m.cloud.Delete(ctx, class, name.(string), nodeID.(string)); err != nil {
			// we just logging in case of we are not able to delete the cloud
			m.logger.WithFields(logrus.Fields{
				"action": "deleting_tenant_from_cloud",
				"error":  err,
				"name":   class,
				"tenant": name,
			}).Error("deleting")
		}
		return true
	})
}
