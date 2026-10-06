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

package sharding

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/storobj"
)

var errAny = errors.New("anyErr")

func TestQueryReplica(t *testing.T) {
	var (
		ctx                      = context.Background()
		canceledCtx, cancledFunc = context.WithCancel(ctx)
	)
	cancledFunc()
	doIf := func(targetNode string) func(node, host string) (interface{}, error) {
		return func(node, host string) (interface{}, error) {
			if node != targetNode {
				return nil, errAny
			}
			return node, nil
		}
	}
	tests := []struct {
		ctx        context.Context
		resolver   fakeNodeResolver
		schema     fakeSchema
		targetNode string
		success    bool
		name       string
	}{
		{
			ctx, newFakeResolver(0, 0), newFakeSchema(0, 0), "N0", false, "empty schema",
		},
		{
			ctx, newFakeResolver(0, 1), newFakeSchema(1, 2), "N2", false, "unresolved name",
		},
		{
			ctx, newFakeResolver(0, 1), newFakeSchema(0, 1), "N0", true, "one replica",
		},
		{
			ctx, newFakeResolver(0, 9), newFakeSchema(0, 9), "N2", true, "random selection",
		},
		{
			canceledCtx, newFakeResolver(0, 9), newFakeSchema(0, 9), "N2", false, "canceled",
		},
	}

	for _, test := range tests {
		rindex := RemoteIndex{"C", &test.schema, nil, &test.resolver}
		got, lastNode, err := rindex.queryReplicas(test.ctx, "S", doIf(test.targetNode))
		if !test.success {
			if got != nil {
				t.Errorf("%s: want: nil, got: %v", test.name, got)
			} else if err == nil {
				t.Errorf("%s: must return an error", test.name)
			}
			continue
		}
		if lastNode != test.targetNode {
			t.Errorf("%s: last responding node want:%s got:%s", test.name, test.targetNode, lastNode)
		}
	}
}

// TestShardReadsFailOverToReadyReplica verifies each read returns the answer of
// N1, the only ready replica, whichever replica readFromReplicas tries first.
func TestShardReadsFailOverToReadyReplica(t *testing.T) {
	ctx := context.Background()
	id := strfmt.UUID("8c7c6a2e-3b5c-4b0e-9a3c-1f2d3e4f5a6b")
	obj := &storobj.Object{Object: models.Object{ID: id}}
	tests := []struct {
		name string
		read func(ri *RemoteIndex) (any, error)
		want any
	}{
		{"GetObject", func(ri *RemoteIndex) (any, error) {
			return ri.GetObject(ctx, "S", id, search.SelectProperties{}, additional.Properties{})
		}, obj},
		{"MultiGetObjects", func(ri *RemoteIndex) (any, error) {
			return ri.MultiGetObjects(ctx, "S", []strfmt.UUID{id})
		}, []*storobj.Object{obj}},
		{"Exists", func(ri *RemoteIndex) (any, error) {
			return ri.Exists(ctx, "S", id)
		}, true},
		{"FindUUIDs", func(ri *RemoteIndex) (any, error) {
			return ri.FindUUIDs(ctx, "S", nil, 10)
		}, []strfmt.UUID{id}},
		{"GetShardQueueSize", func(ri *RemoteIndex) (any, error) {
			return ri.GetShardQueueSize(ctx, "S")
		}, int64(7)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := newFakeSchema(0, 2)
			resolver := newFakeResolver(0, 2)
			ri := NewRemoteIndex("C", &schema, &resolver, readyOnlyOn{readyHost: "H1", obj: obj})

			got, err := tt.read(ri)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

// readyOnlyOn answers shard reads from readyHost and fails them on every other
// host, as a replica does while its shard is loading or recovering.
type readyOnlyOn struct {
	RemoteIndexClient
	readyHost string
	obj       *storobj.Object
}

func (c readyOnlyOn) check(host string) error {
	if host != c.readyHost {
		return errAny
	}
	return nil
}

func (c readyOnlyOn) GetObject(_ context.Context, host, _, _ string, _ strfmt.UUID,
	_ search.SelectProperties, _ additional.Properties,
) (*storobj.Object, error) {
	if err := c.check(host); err != nil {
		return nil, err
	}
	return c.obj, nil
}

func (c readyOnlyOn) MultiGetObjects(_ context.Context, host, _, _ string, _ []strfmt.UUID,
) ([]*storobj.Object, error) {
	if err := c.check(host); err != nil {
		return nil, err
	}
	return []*storobj.Object{c.obj}, nil
}

func (c readyOnlyOn) Exists(_ context.Context, host, _, _ string, _ strfmt.UUID) (bool, error) {
	if err := c.check(host); err != nil {
		return false, err
	}
	return true, nil
}

func (c readyOnlyOn) FindUUIDs(_ context.Context, host, _, _ string, _ *filters.LocalFilter, _ int,
) ([]strfmt.UUID, error) {
	if err := c.check(host); err != nil {
		return nil, err
	}
	return []strfmt.UUID{c.obj.ID()}, nil
}

func (c readyOnlyOn) GetShardQueueSize(_ context.Context, host, _, _ string) (int64, error) {
	if err := c.check(host); err != nil {
		return 0, err
	}
	return 7, nil
}

func newFakeResolver(fromNode, toNode int) fakeNodeResolver {
	m := make(map[string]string, toNode-fromNode)
	for i := fromNode; i < toNode; i++ {
		m[fmt.Sprintf("N%d", i)] = fmt.Sprintf("H%d", i)
	}
	return fakeNodeResolver{m}
}

func newFakeSchema(fromNode, toNode int) fakeSchema {
	nodes := make([]string, 0, toNode-fromNode)
	for i := fromNode; i < toNode; i++ {
		nodes = append(nodes, fmt.Sprintf("N%d", i))
	}
	return fakeSchema{nodes}
}

type fakeNodeResolver struct {
	rTable map[string]string
}

func (r *fakeNodeResolver) AllHostnames() []string {
	hosts := make([]string, 0, len(r.rTable))

	for _, h := range r.rTable {
		hosts = append(hosts, h)
	}

	return hosts
}

func (f *fakeNodeResolver) NodeHostname(name string) (string, bool) {
	host, ok := f.rTable[name]
	return host, ok
}

type fakeSchema struct {
	nodes []string
}

func (f *fakeSchema) ShardOwner(class, shard string) (string, error) {
	return "", nil
}

func (f *fakeSchema) ShardReplicas(class, shard string) ([]string, error) {
	return f.nodes, nil
}
