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
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestTelemetry_BuildPayload(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		t.Run("on init", func(t *testing.T) {
			tel, sg, mp := newTestTelemeter()
			sg.On("LocalNodeStatus", context.Background(), "", verbosity.OutputVerbose).Return(
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
			payload, err := tel.buildPayload(context.Background(), PayloadType.Init)
			assert.Nil(t, err)
			assert.Equal(t, tel.machineID, payload.MachineID)
			assert.Equal(t, PayloadType.Init, payload.Type)
			assert.Equal(t, config.ServerVersion, payload.Version)
			assert.Equal(t, "module-1,module-2", payload.Modules)
			assert.Equal(t, int64(0), payload.NumObjects)
			assert.Equal(t, runtime.GOOS, payload.OS)
			assert.Equal(t, runtime.GOARCH, payload.Arch)
		})

		t.Run("on update", func(t *testing.T) {
			tel, sg, mp := newTestTelemeter()
			sg.On("LocalNodeStatus", context.Background(), "", verbosity.OutputVerbose).Return(
				&models.NodeStatus{
					Stats: &models.NodeStats{
						ObjectCount: 1000,
					},
				})
			mp.On("GetMeta").Return(map[string]interface{}{}, nil)
			payload, err := tel.buildPayload(context.Background(), PayloadType.Update)
			assert.Nil(t, err)
			assert.Equal(t, tel.machineID, payload.MachineID)
			assert.Equal(t, PayloadType.Update, payload.Type)
			assert.Equal(t, config.ServerVersion, payload.Version)
			assert.Equal(t, "", payload.Modules)
			assert.Equal(t, int64(1000), payload.NumObjects)
			assert.Equal(t, runtime.GOOS, payload.OS)
			assert.Equal(t, runtime.GOARCH, payload.Arch)
		})

		t.Run("on terminate", func(t *testing.T) {
			tel, sg, mp := newTestTelemeter()
			sg.On("LocalNodeStatus", context.Background(), "", verbosity.OutputVerbose).Return(
				&models.NodeStatus{
					Stats: &models.NodeStats{
						ObjectCount: 300_000_000_000,
					},
				})
			mp.On("GetMeta").Return(nil, nil)
			payload, err := tel.buildPayload(context.Background(), PayloadType.Terminate)
			assert.Nil(t, err)
			assert.Equal(t, tel.machineID, payload.MachineID)
			assert.Equal(t, PayloadType.Terminate, payload.Type)
			assert.Equal(t, config.ServerVersion, payload.Version)
			assert.Equal(t, "", payload.Modules)
			assert.Equal(t, int64(300_000_000_000), payload.NumObjects)
			assert.Equal(t, runtime.GOOS, payload.OS)
			assert.Equal(t, runtime.GOARCH, payload.Arch)
		})
	})

	t.Run("failure path", func(t *testing.T) {
		t.Run("fail to get enabled modules", func(t *testing.T) {
			tel, sg, mp := newTestTelemeter()
			sg.On("LocalNodeStatus", context.Background(), "", verbosity.OutputVerbose).Return(
				&models.NodeStatus{Stats: &models.NodeStats{ObjectCount: 10}})
			mp.On("GetMeta").Return(nil, errors.New("FAILURE"))
			payload, err := tel.buildPayload(context.Background(), PayloadType.Terminate)
			assert.Nil(t, payload)
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), "get enabled modules")
		})

		t.Run("fail to get node status", func(t *testing.T) {
			tel, sg, mp := newTestTelemeter()
			sg.On("LocalNodeStatus", context.Background(), "", verbosity.OutputVerbose).Return(nil)
			mp.On("GetMeta").Return(
				map[string]interface{}{
					"module-1": nil,
					"module-2": nil,
				})
			payload, err := tel.buildPayload(context.Background(), PayloadType.Terminate)
			assert.Nil(t, payload)
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), "get object count")
		})

		t.Run("fail to get node status stats", func(t *testing.T) {
			tel, sg, mp := newTestTelemeter()
			sg.On("LocalNodeStatus", context.Background(), "", verbosity.OutputVerbose).Return(&models.NodeStatus{})
			mp.On("GetMeta").Return(
				map[string]interface{}{
					"module-1": nil,
					"module-2": nil,
				})
			payload, err := tel.buildPayload(context.Background(), PayloadType.Terminate)
			assert.Nil(t, payload)
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), "get object count")
		})
	})
}

func TestTelemetry_WithConsumer(t *testing.T) {
	config.ServerVersion = "X.X.X"
	server := httptest.NewServer(&testConsumer{t})
	defer server.Close()

	consumerURL := fmt.Sprintf("%s/weaviate-telemetry", server.URL)
	opts := []telemetryOpt{
		withConsumerURL(consumerURL),
		withPushInterval(100 * time.Millisecond),
	}
	tel, sg, mp := newTestTelemeter(opts...)

	sg.On("LocalNodeStatus", context.Background(), "", verbosity.OutputVerbose).Return(
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

	err := tel.Start(context.Background())
	require.Nil(t, err)

	ticker := time.NewTicker(100 * time.Millisecond)
	start := time.Now()
	wait := make(chan struct{})
	go func() {
		for range ticker.C {
			if time.Since(start) > time.Second {
				err = tel.Stop(context.Background())
				assert.Nil(t, err)
				wait <- struct{}{}
			}
		}
	}()
	<-wait
}

type telemetryOpt func(*Telemeter)

func withConsumerURL(url string) telemetryOpt {
	encoded := base64.StdEncoding.EncodeToString([]byte(url))
	return func(tel *Telemeter) {
		tel.consumer = encoded
	}
}

func withPushInterval(interval time.Duration) telemetryOpt {
	return func(tel *Telemeter) {
		tel.pushInterval = interval
	}
}

func newTestTelemeter(opts ...telemetryOpt,
) (*Telemeter, *fakeNodesStatusGetter, *fakeModulesProvider,
) {
	sg := &fakeNodesStatusGetter{}
	mp := &fakeModulesProvider{}
	logger, _ := test.NewNullLogger()
	tel := New(sg, mp, logger)
	for _, opt := range opts {
		opt(tel)
	}
	return tel, sg, mp
}

type testConsumer struct {
	t *testing.T
}

func (h *testConsumer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	assert.Equal(h.t, "/weaviate-telemetry", r.URL.String())
	assert.Equal(h.t, http.MethodPost, r.Method)
	b, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	require.Nil(h.t, err)

	var payload Payload
	err = json.Unmarshal(b, &payload)
	require.Nil(h.t, err)

	assert.NotEmpty(h.t, payload.MachineID)
	assert.Contains(h.t, []string{
		PayloadType.Init,
		PayloadType.Update,
		PayloadType.Terminate,
	}, payload.Type)
	assert.Equal(h.t, config.ServerVersion, payload.Version)
	assert.NotEmpty(h.t, payload.Modules)
	if payload.Type == PayloadType.Init {
		assert.Zero(h.t, payload.NumObjects)
	} else {
		assert.NotZero(h.t, payload.NumObjects)
	}
	assert.Equal(h.t, runtime.GOOS, payload.OS)
	assert.Equal(h.t, runtime.GOARCH, payload.Arch)

	h.t.Logf("request body: %s", string(b))
	w.WriteHeader(http.StatusOK)
}
