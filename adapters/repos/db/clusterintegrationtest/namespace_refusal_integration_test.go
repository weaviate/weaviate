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

package clusterintegrationtest

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	api "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/namespaces"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

// namespaceInState is an Exister reporting every namespace in the state the
// test last set.
type namespaceInState struct {
	mu    sync.Mutex
	state api.NamespaceState
}

func (e *namespaceInState) GetNamespace(name string) (api.Namespace, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return api.Namespace{Name: name, State: e.state}, true
}

func (e *namespaceInState) set(state api.NamespaceState) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.state = state
}

// TestDistributedSearchRefusedBySuspendedOwner searches through node-0, which
// still reads the namespace as active, after node-1 has seen it suspended.
func TestDistributedSearchRefusedBySuspendedOwner(t *testing.T) {
	ctx := context.Background()
	shardState := multiShardState(2)
	cls := class()
	cls.Class = namespacing.QualifiedName("acme", distributedClass)

	var nodes []*node
	existers := make([]*namespaceInState, 2)
	for i := range existers {
		existers[i] = &namespaceInState{state: api.NamespaceStateActive}
		nodes = append(nodes, &node{name: fmt.Sprintf("node-%d", i), namespacesExister: existers[i]})
	}
	dirName := t.TempDir()
	for _, n := range nodes {
		n.init(t, dirName, &nodes, shardState, false)
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			n.repo.Shutdown(context.Background())
		}
	})
	for _, n := range nodes {
		require.NoError(t, n.migrator.AddClass(ctx, cls))
		n.schemaManager.schema.Objects.Classes = append(n.schemaManager.schema.Objects.Classes, cls)
	}

	data := exampleData(20)
	for _, obj := range data {
		obj.Class = cls.Class
	}
	res, err := nodes[0].repo.BatchPutObjects(ctx, dataAsBatch(data), nil, 0)
	require.NoError(t, err)
	for _, obj := range res {
		require.NoError(t, obj.Err)
	}

	search := func() error {
		_, err := nodes[0].repo.VectorSearch(ctx, dto.GetParams{
			Pagination: &filters.Pagination{Limit: 10},
			ClassName:  cls.Class,
		}, []string{""}, []models.Vector{[]float32(data[0].Vector)})
		return err
	}
	require.NoError(t, search())

	existers[1].set(api.NamespaceStateSuspended)
	start := time.Now()
	err = search()
	require.ErrorIs(t, err, namespaces.ErrNamespaceSuspended)
	assert.Less(t, time.Since(start), 2*time.Second)
}
