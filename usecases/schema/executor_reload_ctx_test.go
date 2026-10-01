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

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestExecutorReloadLocalDBPropagatesStartupTags(t *testing.T) {
	cases := []struct {
		name     string
		ctx      context.Context
		wantFlag bool
	}{
		{name: "node kept its raft state", ctx: context.Background()},
		{name: "node started without raft state", ctx: enterrors.WithStartedWithoutRaftState(context.Background()), wantFlag: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got context.Context
			migrator := &fakeMigrator{onUpdateIndex: func(ctx context.Context) { got = ctx }}
			migrator.On("UpdateIndex", mock.Anything, mock.Anything).Return(nil)
			x := newMockExecutor(migrator, &fakeSchemaManager{})

			err := x.ReloadLocalDB(tc.ctx, []api.UpdateClassRequest{{Class: &models.Class{Class: "A"}, State: &sharding.State{}}})
			require.NoError(t, err)
			require.NotNil(t, got)
			require.True(t, enterrors.IsStartupDBLoad(got))
			require.Equal(t, tc.wantFlag, enterrors.IsStartedWithoutRaftState(got))
		})
	}
}
