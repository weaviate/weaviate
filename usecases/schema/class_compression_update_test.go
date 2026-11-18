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

	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/entities/versioned"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

// TestUpdateClassKeepsEnabledHnswCompression: an HNSW index cannot be
// recompressed, so an update used to leave the index as it was while the
// schema reported whatever the body said (and a shard whose schema said BQ
// over RQ data failed to load on restart). An update that names no
// compression must keep the stored one, as a body built by a client does not
// carry the server default; one that names another must be rejected.
func TestUpdateClassKeepsEnabledHnswCompression(t *testing.T) {
	enabled := func(name string, extra map[string]interface{}) map[string]interface{} {
		block := map[string]interface{}{"enabled": true}
		for k, v := range extra {
			block[k] = v
		}
		return map[string]interface{}{name: block}
	}
	for _, tt := range []struct {
		name            string
		create          map[string]interface{}
		update          map[string]interface{}
		wantErr         bool
		wantCompression string
		wantTracked     bool
	}{
		{name: "default kept when the update names none", wantCompression: "rq-4", wantTracked: true},
		{name: "default kept when the update repeats it", update: enabled("rq", map[string]interface{}{"bits": float64(4)}), wantCompression: "rq-4", wantTracked: true},
		{name: "default cannot be switched to sq", update: enabled("sq", nil), wantErr: true},
		{name: "default cannot be switched to pq", update: enabled("pq", nil), wantErr: true},
		{name: "default cannot be switched to bq", update: enabled("bq", nil), wantErr: true},
		{name: "default cannot change rq bits", update: enabled("rq", map[string]interface{}{"bits": float64(8)}), wantErr: true},
		{name: "explicit bq kept when the update names none", create: enabled("bq", nil), wantCompression: "bq"},
		{name: "explicit bq cannot be switched to rq", create: enabled("bq", nil), update: enabled("rq", nil), wantErr: true},
		{name: "explicit pq cannot be switched to sq", create: enabled("pq", nil), update: enabled("sq", nil), wantErr: true},
		{name: "uncompressed can enable sq", create: map[string]interface{}{"skipDefaultQuantization": true}, update: enabled("sq", nil), wantCompression: "sq"},
		{name: "uncompressed stays uncompressed", create: map[string]interface{}{"skipDefaultQuantization": true}, wantCompression: "none"},
	} {
		for _, indexType := range []string{"hnsw", "dynamic"} {
			for _, shape := range rqClassShapes {
				t.Run(tt.name+"/"+indexType+"/"+shape.name, func(t *testing.T) {
					handler, manager := newTestHandlerAndManagerWithRealVectorConfigParser(t)
					handler.asyncIndexingEnabled = true // dynamic requires it
					handler.config.DefaultQuantization = runtime.NewDynamicValue("rq-4")
					handler.schemaConfig.MaximumAllowedCollectionsCount = runtime.NewDynamicValue(-1)

					indexConfig := func(cfg map[string]interface{}) map[string]interface{} {
						if cfg == nil {
							cfg = map[string]interface{}{}
						}
						if indexType == "dynamic" {
							return map[string]interface{}{"hnsw": cfg}
						}
						return cfg
					}
					var stored, updated *models.Class
					manager.On("AddClass", mock.Anything, mock.Anything).
						Run(func(args mock.Arguments) { stored = args.Get(0).(*models.Class) }).
						Return(nil)
					initial := shape.class(indexType, indexConfig(tt.create))
					initial.ReplicationConfig = &models.ReplicationConfig{Factor: 1}
					_, _, err := handler.AddClass(context.Background(), nil, initial)
					require.NoError(t, err)
					require.NotNil(t, stored)

					manager.On("ReadOnlyClass", stored.Class, mock.Anything).Return(stored)
					manager.On("ReadOnlyClassesFromLeader", []string{stored.Class}).
						Return(map[string]versioned.Class{stored.Class: {Class: stored}}, nil)
					manager.On("UpdateClass", mock.Anything, mock.Anything, command.ClassUpdateOriginUser).
						Run(func(args mock.Arguments) { updated = args.Get(0).(*models.Class) }).
						Return(nil)

					update := shape.class(indexType, indexConfig(tt.update))
					update.ReplicationConfig = &models.ReplicationConfig{Factor: 1}
					err = handler.UpdateClass(context.Background(), nil, stored.Class, update)
					if tt.wantErr {
						require.ErrorIs(t, err, ErrValidation)
						require.Nil(t, updated, "a rejected update must not reach the schema")
						return
					}
					require.NoError(t, err)
					require.NotNil(t, updated)

					cfg := updated.VectorIndexConfig
					if shape.name == "named" {
						cfg = updated.VectorConfig["v"].VectorIndexConfig
					}
					if dyn, ok := cfg.(dynamic.UserConfig); ok {
						cfg = dyn.HnswUC
					}
					hnswCfg := cfg.(hnsw.UserConfig)
					require.Equal(t, tt.wantCompression, compressionFromHnsw(hnswCfg))
					require.Equal(t, tt.wantTracked, hnswCfg.TrackDefaultQuantization)
				})
			}
		}
	}
}
