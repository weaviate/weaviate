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

package telemetry

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestTelemetry(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		t.Run("on init", func(t *testing.T) {
			tel, sg, mp := newTestTelemeter()
			sg.On("LocalNodeStatus", "", verbosity.OutputMinimal).Return(
				&models.NodeStatus{
					Stats: &models.NodeStats{
						ObjectCount: 100,
					},
				})
			mp.On("GetMeta").Return(
				map[string]interface{}{
					"module-1": nil,
					"module-2": nil,
				})
			payload, err := tel.Push(context.Background(), PayloadType.Init)
			assert.Nil(t, err)
			assert.Equal(t, tel.MachineID, payload.MachineID)
			assert.Equal(t, PayloadType.Init, payload.Type)
			assert.Equal(t, config.ServerVersion, payload.Version)
			assert.Equal(t, "module-1,module-2", payload.Modules)
			assert.Equal(t, int64(100), payload.NumObjects)
		})

		t.Run("on update", func(t *testing.T) {
			tel, sg, mp := newTestTelemeter()
			sg.On("LocalNodeStatus", "", verbosity.OutputMinimal).Return(
				&models.NodeStatus{
					Stats: &models.NodeStats{
						ObjectCount: 1000,
					},
				})
			mp.On("GetMeta").Return(map[string]interface{}{}, nil)
			payload, err := tel.Push(context.Background(), PayloadType.Update)
			assert.Nil(t, err)
			assert.Equal(t, tel.MachineID, payload.MachineID)
			assert.Equal(t, PayloadType.Update, payload.Type)
			assert.Equal(t, config.ServerVersion, payload.Version)
			assert.Equal(t, "", payload.Modules)
			assert.Equal(t, int64(1000), payload.NumObjects)
		})

		t.Run("on terminate", func(t *testing.T) {
			tel, sg, mp := newTestTelemeter()
			sg.On("LocalNodeStatus", "", verbosity.OutputMinimal).Return(
				&models.NodeStatus{
					Stats: &models.NodeStats{
						ObjectCount: 300_000_000_000,
					},
				})
			mp.On("GetMeta").Return(nil, nil)
			payload, err := tel.Push(context.Background(), PayloadType.Terminate)
			assert.Nil(t, err)
			assert.Equal(t, tel.MachineID, payload.MachineID)
			assert.Equal(t, PayloadType.Terminate, payload.Type)
			assert.Equal(t, config.ServerVersion, payload.Version)
			assert.Equal(t, "", payload.Modules)
			assert.Equal(t, int64(300_000_000_000), payload.NumObjects)
		})
	})

	t.Run("failure path", func(t *testing.T) {
		t.Run("fail to get enabled modules", func(t *testing.T) {
			tel, sg, mp := newTestTelemeter()
			sg.On("LocalNodeStatus", "", verbosity.OutputMinimal).Return(
				&models.NodeStatus{Stats: &models.NodeStats{ObjectCount: 10}})
			mp.On("GetMeta").Return(nil, errors.New("FAILURE"))
			payload, err := tel.Push(context.Background(), PayloadType.Terminate)
			assert.Nil(t, payload)
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), "get enabled modules")
		})

		t.Run("fail to get node status", func(t *testing.T) {
			tel, sg, mp := newTestTelemeter()
			sg.On("LocalNodeStatus", "", verbosity.OutputMinimal).Return(nil)
			mp.On("GetMeta").Return(
				map[string]interface{}{
					"module-1": nil,
					"module-2": nil,
				})
			payload, err := tel.Push(context.Background(), PayloadType.Terminate)
			assert.Nil(t, payload)
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), "get object count")
		})

		t.Run("fail to get node status stats", func(t *testing.T) {
			tel, sg, mp := newTestTelemeter()
			sg.On("LocalNodeStatus", "", verbosity.OutputMinimal).Return(&models.NodeStatus{})
			mp.On("GetMeta").Return(
				map[string]interface{}{
					"module-1": nil,
					"module-2": nil,
				})
			payload, err := tel.Push(context.Background(), PayloadType.Terminate)
			assert.Nil(t, payload)
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), "get object count")
		})
	})
}

func newTestTelemeter() (*Telemeter, *fakeNodesStatusGetter, *fakeModulesProvider) {
	sg := &fakeNodesStatusGetter{}
	mp := &fakeModulesProvider{}
	return New(sg, mp), sg, mp
}
