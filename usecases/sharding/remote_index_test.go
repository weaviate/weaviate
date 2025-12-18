//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package sharding_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
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
		resolver   cluster.NodeSelector
		schema     schema.SchemaReader
		targetNode string
		success    bool
		name       string
	}{
		{
			ctx, cluster.NewMockNodeSelector(t), schema.NewMockSchemaReader(t), "N0", false, "empty schema",
		},
		{
			ctx, cluster.NewMockNodeSelector(t), schema.NewMockSchemaReader(t), "N2", false, "unresolved name",
		},
		{
			ctx, cluster.NewMockNodeSelector(t), schema.NewMockSchemaReader(t), "N0", true, "one replica",
		},
		{
			ctx, cluster.NewMockNodeSelector(t), schema.NewMockSchemaReader(t), "N2", true, "random selection",
		},
		{
			canceledCtx, cluster.NewMockNodeSelector(t), schema.NewMockSchemaReader(t), "N2", false, "canceled",
		},
	}

	for _, test := range tests {
		mockNodeResolver := cluster.NewMockNodeResolver(t)
		mockNodeResolver.EXPECT().AllHostnames().Return([]string{test.targetNode}).Maybe()
		if test.name == "unresolved name" {
			// Simulate failure to resolve any replica hostname
			mockNodeResolver.EXPECT().NodeHostname(mock.Anything).Return("", false)
		} else {
			// Resolve any replica name to a non-empty host and report success
			mockNodeResolver.EXPECT().
				NodeHostname(mock.Anything).
				Return("host", true).
				Maybe()
		}
		rindex := sharding.NewRemoteIndex("C", nil, mockNodeResolver, nil)
		got, lastNode, err := rindex.QueryReplicas(test.ctx, "S", doIf(test.targetNode))
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
