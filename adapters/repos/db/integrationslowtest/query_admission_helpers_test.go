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

//go:build integrationTest

package integrationslowtest

import (
	"testing"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/entities/models"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// admissionRepoParams carries the knobs that differ per test. A nil
// promMetrics registers to the global noop registerer, so its gauges are
// not isolated for reading.
type admissionRepoParams struct {
	budget      int
	maxQueue    int
	disabled    *configRuntime.DynamicValue[bool]
	promMetrics *monitoring.PrometheusMetrics
}

// newAdmissionRepo builds a single-node DB wired for query-admission
// integration tests, with the given classes migrated in. Callers own
// whatever differs per test (budgets, class shapes, metrics, object import).
func newAdmissionRepo(t *testing.T, p admissionRepoParams,
	className string, classes ...*models.Class,
) (*db.DB, db.ShardLike) {
	t.Helper()
	repo, _ := newRepo(t, repoParams{
		promMetrics: p.promMetrics,
		config: func(c *db.Config) {
			c.QueryAdmissionBudget = p.budget
			c.QueryAdmissionMaxQueue = p.maxQueue
			c.QueryAdmissionControlDisabled = p.disabled
		},
	}, classes...)
	return repo, singleShard(t, repo, className)
}
