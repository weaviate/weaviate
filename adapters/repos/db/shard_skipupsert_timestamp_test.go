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

//go:build integrationTest

package db

import (
	"context"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	routerTypes "github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
)

// Empty (but non-nil) properties reach the content-equality path so two versions
// differ only by LastUpdateTimeUnix.
func objWithEmptyPropsAndTime(class string, id strfmt.UUID, updateTime int64) *storobj.Object {
	return &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:                 id,
			Class:              class,
			Properties:         map[string]interface{}{},
			LastUpdateTimeUnix: updateTime,
		},
	}
}

// TestPutIdenticalContentAdvancesUpdateTime reproduces the async-replication
// perpetual-propagation loop: a write whose content is byte-identical to the
// stored object but carries a newer update time must still advance the persisted
// update time, otherwise the update-time-based digest comparison never converges
// and the object is re-propagated on every hashbeat. This is what "multiple
// update requests with the same content" produce across replicas that observe
// the writes at different points.
func TestPutIdenticalContentAdvancesUpdateTime(t *testing.T) {
	ctx := context.Background()
	const class = "SkipUpsertTimestampConvergence"

	const (
		tsMiddle int64 = 2_000
		tsNewer  int64 = 3_000
	)

	tests := []struct {
		name           string
		firstTime      int64
		secondTime     int64
		wantStoredTime int64
	}{
		{name: "newer identical update advances and converges", firstTime: tsMiddle, secondTime: tsNewer, wantStoredTime: tsNewer},
		{name: "equal identical update is a no-op", firstTime: tsMiddle, secondTime: tsMiddle, wantStoredTime: tsMiddle},
		{name: "older identical update does not regress", firstTime: tsNewer, secondTime: tsMiddle, wantStoredTime: tsNewer},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sl, _ := testShard(t, ctx, class)
			s := concreteShard(t, sl)

			require.NoError(t, sl.PutObject(ctx, objWithEmptyPropsAndTime(class, uuidLow, tt.firstTime)))
			require.NoError(t, sl.PutObject(ctx, objWithEmptyPropsAndTime(class, uuidLow, tt.secondTime)))

			obj, err := s.ObjectByID(ctx, uuidLow, nil, additional.Properties{})
			require.NoError(t, err)
			require.NotNil(t, obj)
			assert.Equal(t, tt.wantStoredTime, obj.LastUpdateTimeUnix())

			out, err := s.CompareDigests(ctx, []routerTypes.RepairResponse{
				{ID: string(uuidLow), UpdateTime: tt.wantStoredTime},
			})
			require.NoError(t, err)
			assert.Empty(t, out, "replicas converged; no further propagation expected")
		})
	}
}
