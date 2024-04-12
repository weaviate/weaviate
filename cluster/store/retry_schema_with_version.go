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

package store

import (
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func (rs retrySchema) ClassInfoWithVersion(class string, version uint64) (ci ClassInfo, resultVersion uint64) {
	rs.retry(func(s *schema) error {
		ci, resultVersion = s.ClassInfo(class, version)
		if !ci.Exists {
			return errClassExists
		}
		return nil
	})
	return
}

func (rs retrySchema) MultiTenancyWithVersion(class string, version uint64) (models.MultiTenancyConfig, uint64) {
	return rs.metaClass(class).MultiTenancyConfig()
}

// ReadOnlyClass returns a shallow copy of a class.
// The copy is read-only and should not be modified.
func (rs retrySchema) ReadOnlyClassWithVersion(class string, version uint64) (cls *models.Class, resultVersion uint64) {
	rs.retry(func(s *schema) error {
		if cls, resultVersion = s.ReadOnlyClass(class); cls == nil {
			return errClassNotFound
		}
		return nil
	})
	return
}

// ShardOwner returns the node owner of the specified shard
func (rs retrySchema) ShardOwnerWithVersion(class, shard string, version uint64) (owner string, resultVersion uint64, err error) {
	err = rs.retry(func(s *schema) error {
		owner, err, resultVersion = s.ShardOwner(class, shard)
		return err
	})
	return
}

// ShardFromUUID returns shard name of the provided uuid
func (rs retrySchema) ShardFromUUIDWithVersion(class string, uuid []byte, version uint64) (shard string, resultVersion uint64) {
	rs.retry(func(s *schema) error {
		if shard, resultVersion = s.ShardFromUUID(class, uuid); shard == "" {
			return errClassNotFound
		}
		return nil
	})
	return
}

// ShardReplicas returns the replica nodes of a shard
func (rs retrySchema) ShardReplicasWithVersion(class, shard string, version uint64) (nodes []string, resultVersion uint64, err error) {
	rs.retry(func(s *schema) error {
		nodes, err, resultVersion = s.ShardReplicas(class, shard)
		return err
	})
	return
}

// TenantShard returns shard name for the provided tenant and its activity status
func (rs retrySchema) TenantShardWithVersion(class, tenant string, version uint64) (name string, st string, resultVersion uint64) {
	rs.retry(func(s *schema) error {
		if name, st, resultVersion = s.TenantShard(class, tenant); name == "" {
			return errShardNotFound
		}
		return nil
	})

	return
}

func (rs retrySchema) CopyShardingStateWithVersion(class string, version uint64) (ss *sharding.State, resultVersion uint64) {
	rs.retry(func(s *schema) error {
		if ss, resultVersion = s.CopyShardingState(class); ss == nil {
			return errClassNotFound
		}
		return nil
	})
	return
}
