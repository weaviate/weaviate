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

package hybrid

import (
	"context"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func newFakeTargetVectorParamHelper() *fakeTargetVectorParamHelper {
	return &fakeTargetVectorParamHelper{}
}

func newFakeSchemaManager() *fakeSchemaManager {
	return &fakeSchemaManager{
		schema: schema.Schema{Objects: &models.Schema{}},
	}
}

type fakeTargetVectorParamHelper struct{}

func (*fakeTargetVectorParamHelper) GetTargetVectorOrDefault(sch schema.Schema, className string, targetVector []string) ([]string, error) {
	return targetVector, nil
}

type fakeSchemaManager struct {
	schema schema.Schema
}

func (f *fakeSchemaManager) GetSchemaSkipAuth() schema.Schema {
	return f.schema
}

func (f *fakeSchemaManager) ReadOnlyClass(class string) *models.Class {
	return f.schema.GetClass(class)
}

func (f *fakeSchemaManager) CopyShardingState(class string) *sharding.State {
	return nil
}

func (f *fakeSchemaManager) ShardOwner(class, shard string) (string, error) {
	return "", nil
}

func (f *fakeSchemaManager) ShardReplicas(class, shard string) ([]string, error) {
	return []string{}, nil
}

func (f *fakeSchemaManager) RestoreClass(ctx context.Context, d *backup.ClassDescriptor, nodeMapping map[string]string) error {
	return nil
}

func (f *fakeSchemaManager) Nodes() []string {
	return []string{"NOT SET"}
}

func (f *fakeSchemaManager) NodeName() string {
	return ""
}

func (f *fakeSchemaManager) ClusterHealthScore() int {
	return 0
}

func (f *fakeSchemaManager) Statistics() map[string]any {
	return nil
}

func (f *fakeSchemaManager) StorageCandidates() []string {
	return []string{}
}

func (f *fakeSchemaManager) ResolveParentNodes(_ string, shard string,
) (map[string]string, error) {
	return nil, nil
}

func (f *fakeSchemaManager) ShardFromUUID(class string, uuid []byte) string {
	return ""
}

func (f *fakeSchemaManager) TenantsShards(_ context.Context, class string, tenants ...string) (map[string]string, error) {
	return nil, nil
}

func (f *fakeSchemaManager) OptimisticTenantStatus(_ context.Context, class string, tenant string) (map[string]string, error) {
	res := map[string]string{}
	res[tenant] = models.TenantActivityStatusHOT
	return res, nil
}
