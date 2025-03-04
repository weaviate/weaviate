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

package replication_test

import (
	"encoding/json"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication"
	"github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/fakes"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestReplicate(t *testing.T) {
	nodeId0 := "weaviate-0"
	nodeId1 := "weaviate-1"

	cls := &models.Class{Class: "C1", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true}}
	shardingState := &sharding.State{Physical: map[string]sharding.Physical{"T1": {
		Name:           "T1",
		BelongsToNodes: []string{nodeId0},
	}, "T2": {
		Name:           "T2",
		BelongsToNodes: []string{nodeId1},
	}}}

	indexer := fakes.NewMockSchemaExecutor()
	parser := fakes.NewMockParser()
	parser.On("ParseClass", mock.Anything).Return(nil)
	indexer.On("TriggerSchemaUpdateCallbacks").Return()
	schemaManager := schema.NewSchemaManager(nodeId0, indexer, parser, prometheus.NewPedanticRegistry(), logrus.New())
	_ = replication.NewManager(logrus.New(), schemaManager.NewSchemaReader())

	err := schemaManager.AddClass(&api.ApplyRequest{
		Type: api.ApplyRequest_TYPE_ADD_CLASS,
		SubCommand: subCmdAsBytes(&api.AddClassRequest{
			Class: cls,
			State: shardingState,
		}),
	}, nodeId0, true, false)
	require.NoError(t, err)
}

func subCmdAsBytes(jsonSubCmd interface{}) []byte {
	var (
		subData []byte
		err     error
	)
	subData, err = json.Marshal(jsonSubCmd)
	if err != nil {
		panic("json.Marshal( " + err.Error())
	}

	return subData
}
