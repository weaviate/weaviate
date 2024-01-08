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

package sharding

import (
	"context"
	"errors"
	"fmt"
	"testing"
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
