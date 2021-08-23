//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

// +build integrationTest

package db

import (
	"encoding/json"

	"github.com/semi-technologies/weaviate/entities/schema"
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
	config, err := sharding.ParseConfig(nil)
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
	})
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
