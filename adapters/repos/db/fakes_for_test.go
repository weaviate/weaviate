//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

//go:build integrationTest
// +build integrationTest

package db

import (
	"context"
	"encoding/json"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/entities/storobj"
	"github.com/semi-technologies/weaviate/usecases/objects"
	"github.com/semi-technologies/weaviate/usecases/sharding"
)

type fakeSchemaGetter struct {
	schema     schema.Schema
	shardState *sharding.State
}

func (f *fakeSchemaGetter) GetSchemaSkipAuth() schema.Schema {
	return f.schema
}

func (f *fakeSchemaGetter) ShardingState(class string) *sharding.State {
	return f.shardState
}

func singleShardState() *sharding.State {
	config, err := sharding.ParseConfig(nil, 1)
	if err != nil {
		panic(err)
	}

	s, err := sharding.InitState("test-index", config, fakeNodes{[]string{"node1"}})
	if err != nil {
		panic(err)
	}

	return s
}

func multiShardState() *sharding.State {
	config, err := sharding.ParseConfig(map[string]interface{}{
		"desiredCount": json.Number("3"),
	}, 1)
	if err != nil {
		panic(err)
	}

	s, err := sharding.InitState("multi-shard-test-index", config,
		fakeNodes{[]string{"node1"}})
	if err != nil {
		panic(err)
	}

	return s
}

type fakeNodes struct {
	nodes []string
}

func (f fakeNodes) AllNames() []string {
	return f.nodes
}

func (f fakeNodes) LocalName() string {
	return f.nodes[0]
}

type fakeRemoteClient struct{}

func (f *fakeRemoteClient) BatchPutObjects(ctx context.Context, hostName, indexName,
	shardName string, obj []*storobj.Object) []error {
	return nil
}

func (f *fakeRemoteClient) PutObject(ctx context.Context, hostName, indexName,
	shardName string, obj *storobj.Object) error {
	return nil
}

func (f *fakeRemoteClient) GetObject(ctx context.Context, hostName, indexName,
	shardName string, id strfmt.UUID, props search.SelectProperties,
	additional additional.Properties) (*storobj.Object, error) {
	return nil, nil
}

func (f *fakeRemoteClient) Exists(ctx context.Context, hostName, indexName,
	shardName string, id strfmt.UUID) (bool, error) {
	return false, nil
}

func (f *fakeRemoteClient) DeleteObject(ctx context.Context, hostName, indexName,
	shardName string, id strfmt.UUID) error {
	return nil
}

func (f *fakeRemoteClient) MergeObject(ctx context.Context, hostName, indexName,
	shardName string, mergeDoc objects.MergeDocument) error {
	return nil
}

func (f *fakeRemoteClient) MultiGetObjects(ctx context.Context, hostName, indexName,
	shardName string, ids []strfmt.UUID) ([]*storobj.Object, error) {
	return nil, nil
}

func (f *fakeRemoteClient) SearchShard(ctx context.Context, hostName, indexName,
	shardName string, vector []float32, limit int, filters *filters.LocalFilter,
	additional additional.Properties) ([]*storobj.Object, []float32, error) {
	return nil, nil, nil
}

func (f *fakeRemoteClient) Aggregate(ctx context.Context, hostName, indexName,
	shardName string, params aggregation.Params) (*aggregation.Result, error) {
	return nil, nil
}

func (f *fakeRemoteClient) BatchAddReferences(ctx context.Context, hostName,
	indexName, shardName string, refs objects.BatchReferences) []error {
	return nil
}

type fakeNodeResolver struct{}

func (f *fakeNodeResolver) NodeHostname(string) (string, bool) {
	return "", false
}
