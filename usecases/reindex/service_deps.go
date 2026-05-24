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

package reindex

import (
	"context"
	"sync"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
)

// Deps are the production-side hooks the [Service] needs. Each field
// is a narrow interface defined in this package so unit tests can
// substitute a fake without standing up the real cluster + DB.
type Deps struct {
	Cluster       ClusterService
	DB            DatabaseOps
	SchemaManager SchemaManager
	Provider      ReindexProviderOps
	SubmitLocks   SubmitLocker
}

// SubmitLocker hands out per-(collection, property) mutexes. The
// concrete implementation is shared across the reindex submit handler
// and the DELETE /properties/{prop}/index handler so the two
// serialize against each other.
type SubmitLocker interface {
	SubmitLockFor(collection, property string) *sync.Mutex
}

// ClusterService is the slice of [cluster.Service] the reindex Submit /
// Cancel paths consume.
type ClusterService interface {
	ListDistributedTasks(ctx context.Context) (map[string][]*distributedtask.Task, error)
	AddDistributedTaskWithBarrier(ctx context.Context, namespace, taskID string, payload any, unitIDs []string, withPrepBarrier bool) error
	AddDistributedTaskWithGroupsBarrier(ctx context.Context, namespace, taskID string, payload any, unitSpecs []distributedtask.UnitSpec, withPrepBarrier bool) error
	CancelDistributedTask(ctx context.Context, namespace, taskID string, version uint64) error
}

// DatabaseOps groups every database call the service makes. Kept
// narrow on purpose: substitution in tests is cheap because each
// method is small.
type DatabaseOps interface {
	ShardOwnershipReader
	SearchableBucketStrategyReader
	CleanStalePartialReindexState(ctx context.Context, collection, propName, indexType string) error
}

// SchemaManager is the slice of [schemaUC.Manager] the reindex service
// reads. Listed by class lookup only — the service never mutates the
// schema directly; mutations go through the distributed-task pipeline.
type SchemaManager interface {
	ReadOnlyClass(className string) *models.Class
}

// ReindexProviderOps is the slice of [reindex.ReindexProvider] the
// cancel path needs. Distinct interface so a service constructed at
// startup before the provider is wired up can still be invoked for
// validate-only calls.
type ReindexProviderOps interface {
	WaitForLocalTaskDrain(ctx context.Context, desc distributedtask.TaskDescriptor) error
}

// noopLocker is the fallback locker used when no production
// implementation is wired. Every "lock" is an independent throwaway,
// so tests that don't care about lock semantics skip the bookkeeping.
type noopLocker struct{}

func (noopLocker) SubmitLockFor(string, string) *sync.Mutex { return &sync.Mutex{} }
