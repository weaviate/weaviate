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
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	command "github.com/weaviate/weaviate/cluster/proto/api"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/fakes"
)

type ctxIndexer struct {
	recordingIndexer
	ctx context.Context
}

func (c *ctxIndexer) ReloadLocalDB(ctx context.Context, _ []command.UpdateClassRequest) error {
	c.ctx = ctx
	return nil
}

func TestReloadDBFromSchemaForwardsContext(t *testing.T) {
	idx := &ctxIndexer{}
	parser := fakes.NewMockParser()
	parser.On("ParseClass", mock.Anything).Return(nil)
	sm := NewSchemaManager("node1", idx, parser, prometheus.NewPedanticRegistry(), logrus.New())
	addClass(t, sm, "A")

	sm.ReloadDBFromSchema(enterrors.WithStartedWithoutRaftState(context.Background()))

	require.NotNil(t, idx.ctx)
	require.True(t, enterrors.IsStartedWithoutRaftState(idx.ctx))
}
