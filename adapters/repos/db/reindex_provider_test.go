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

package db

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	entschema "github.com/weaviate/weaviate/entities/schema"
)

func closingIndexWithAnUnvisitedShard(t *testing.T) (*Index, string) {
	t.Helper()
	const tenant = "cold-tenant"
	logger, _ := logrustest.NewNullLogger()

	closingCtx, closeIndex := context.WithCancel(context.Background())
	closeIndex()
	closeRequestedCtx, signalCloseRequested := context.WithCancelCause(context.Background())
	t.Cleanup(func() { signalCloseRequested(nil) })

	idx := &Index{
		Config: IndexConfig{
			RootPath:  t.TempDir(),
			ClassName: entschema.ClassName("ClosingSweep" + uuid.NewString()[:8]),
		},
		closingCtx:           closingCtx,
		closeRequestedCtx:    closeRequestedCtx,
		signalCloseRequested: signalCloseRequested,
		logger:               logger,
	}
	idx.shards.Store(tenant, &LazyLoadShard{
		shardOpts: &deferredShardOpts{name: tenant, index: idx},
	})
	return idx, tenant
}

func TestLookupShardByName(t *testing.T) {
	open := &Index{}
	open.shards.Store("tenant-a", &Shard{})
	closing, closingTenant := closingIndexWithAnUnvisitedShard(t)

	tests := []struct {
		name    string
		idx     *Index
		shard   string
		wantErr string
	}{
		{name: "missing shard", idx: open, shard: "tenant-b", wantErr: `shard "tenant-b" not found`},
		{name: "index closing", idx: closing, shard: closingTenant, wantErr: fmt.Sprintf("shard %q not found", closingTenant)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := lookupShardByName(tt.idx, tt.shard)
			require.EqualError(t, err, tt.wantErr)
		})
	}
}
