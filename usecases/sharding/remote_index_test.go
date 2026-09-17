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
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/objects"
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

// TestDeleteObjectBatchUnreachableShard asserts that a delete that cannot reach
// the shard reports the failure at every id, with the id.
func TestDeleteObjectBatchUnreachableShard(t *testing.T) {
	ids := []strfmt.UUID{
		"00000000-0000-0000-0000-000000000001",
		"00000000-0000-0000-0000-000000000002",
	}

	tests := []struct {
		name    string
		wantErr string
		delete  func() objects.BatchSimpleObjects
	}{
		{
			name:    "shard without owner",
			wantErr: "has no physical shard",
			delete: func() objects.BatchSimpleObjects {
				rindex := NewRemoteIndex("C", &ownerlessSchema{}, &fakeNodeResolver{}, nil)
				return rindex.DeleteObjectBatch(context.Background(), "S", ids, time.Now(), false, 0)
			},
		},
		{
			name:    "owner without host",
			wantErr: "resolve node name",
			delete: func() objects.BatchSimpleObjects {
				rindex := NewRemoteIndex("C", &fakeSchema{}, &fakeNodeResolver{}, nil)
				return rindex.DeleteObjectBatch(context.Background(), "S", ids, time.Now(), false, 0)
			},
		},
		{
			name:    "incoming, class not found",
			wantErr: "local index",
			delete: func() objects.BatchSimpleObjects {
				incoming := NewRemoteIndexIncoming(nil, classlessSchema{}, nil)
				return incoming.DeleteObjectBatch(context.Background(), "C", "S", ids, time.Now(), false, 0)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			results := test.delete()

			require.Len(t, results, len(ids))
			for pos, result := range results {
				require.Equalf(t, ids[pos], result.UUID, "position %d must keep its id", pos)
				require.ErrorContainsf(t, result.Err, test.wantErr, "position %d must carry the failure", pos)
			}
		})
	}
}

type ownerlessSchema struct{ fakeSchema }

func (*ownerlessSchema) ShardOwner(class, shard string) (string, error) {
	return "", errAny
}

type classlessSchema struct{}

func (classlessSchema) ReadOnlyClassWithVersion(ctx context.Context, class string, version uint64) (*models.Class, error) {
	return nil, errAny
}
