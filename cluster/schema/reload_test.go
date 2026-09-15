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

package schema

import (
	"context"
	"runtime"
	"testing"
	"time"
	"weak"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/fakes"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// TestReloadReleasesShardingStateCopies pins that the classes ReloadDBFromSchema
// hands the DB, which shards keep, do not keep its sharding state copies alive.
func TestReloadReleasesShardingStateCopies(t *testing.T) {
	var (
		classes []*models.Class
		states  []weak.Pointer[sharding.State]
	)
	idx := &recordingIndexer{onReload: func(all []command.UpdateClassRequest) {
		for _, u := range all {
			classes = append(classes, u.Class)
			states = append(states, weak.Make(u.State))
		}
	}}
	parser := fakes.NewMockParser()
	parser.On("ParseClass", mock.Anything).Return(nil)
	sm := NewSchemaManager("node1", idx, parser, prometheus.NewPedanticRegistry(), logrus.New())
	addClass(t, sm, "A")
	addClass(t, sm, "B")

	require.NoError(t, sm.ReloadDBFromSchema(context.Background()))

	require.Len(t, states, 2)
	require.Eventually(t, func() bool {
		runtime.GC()
		for _, s := range states {
			if s.Value() != nil {
				return false
			}
		}
		return true
	}, 5*time.Second, 10*time.Millisecond, "a reloaded class keeps its sharding state copy alive")
	runtime.KeepAlive(classes)
}
