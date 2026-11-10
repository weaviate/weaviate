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

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/restrictions"
)

// TestDefaultCompressionIndexTypeUnderAllowList pins the review finding on
// #13656 that the allow-list bypass is wider than RQ on hamming/manhattan:
// flat takes only rq-1, rq-8 and bq as a default, so under an allow-list of
// only rq-4, pq or sq (each seeds the matching default) every new flat index,
// and the flat arm of every dynamic index, would be created uncompressed on
// any distance. Create must reject them and name the index type as the
// cause, not the distance. Indexes the default does apply to, explicit
// allowed compression on the arm, policies admitting "none" and unrestricted
// servers are unaffected.
func TestDefaultCompressionIndexTypeUnderAllowList(t *testing.T) {
	create := func(t *testing.T, allow []string, def string, class *models.Class, configure ...func(*Handler)) (*models.Class, error) {
		t.Helper()
		handler, manager := newTestHandlerAndManagerWithRealVectorConfigParser(t)
		handler.asyncIndexingEnabled = true
		for _, c := range configure {
			c(handler)
		}
		if allow != nil {
			handler.config.Restrictions.AllowedCompressionTypes = runtime.NewDynamicValue(allow)
		}
		handler.config.DefaultQuantization = runtime.NewDynamicValue(def)
		var stored *models.Class
		manager.On("AddClass", mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) { stored = args.Get(0).(*models.Class) }).
			Return(nil).Maybe()
		class.ReplicationConfig = &models.ReplicationConfig{Factor: 1}
		_, _, err := handler.AddClass(context.Background(), nil, class)
		return stored, err
	}
	requireTypeRejection := func(t *testing.T, stored *models.Class, err error, def, armType string) {
		t.Helper()
		require.Error(t, err)
		require.Nil(t, stored, "the class must not be created")
		v, ok := restrictions.AsViolation(err)
		require.True(t, ok, "a restriction violation, got %v", err)
		require.Equal(t, restrictions.RestrictionCompression, v.Restriction)
		require.Equal(t, "none", v.Value)
		require.Contains(t, err.Error(), `default compression "`+def+`" cannot be applied to a `+armType+` index`)
		require.NotContains(t, err.Error(), "cannot compress distance", "the distance is not the cause")
	}

	for _, def := range []string{"rq-4", "pq", "sq"} {
		for _, shape := range rqClassShapes {
			for _, distance := range []string{"cosine", "l2-squared", "dot", "hamming", "manhattan"} {
				prefix := def + "/" + shape.name + "/" + distance
				t.Run(prefix+"/flat rejected", func(t *testing.T) {
					stored, err := create(t, []string{def}, def, shape.class("flat", rqIndexConfig("flat", "", distance, nil)))
					requireTypeRejection(t, stored, err, def, "flat")
				})
				if distance == "cosine" || distance == "l2-squared" || distance == "dot" {
					t.Run(prefix+"/dynamic flat arm rejected", func(t *testing.T) {
						stored, err := create(t, []string{def}, def, shape.class("dynamic", rqIndexConfig("dynamic", "", distance, nil)))
						requireTypeRejection(t, stored, err, def, "flat")
						require.Contains(t, err.Error(), "on the dynamic index's flat arm")
					})
				}
			}
			// no vectorIndexConfig at all, as a JSON body without one arrives;
			// validation sees no parsed config, creation parses the defaults
			for _, metric := range []string{"", "cosine"} {
				t.Run(def+"/"+shape.name+"/flat without index config, default metric "+metric+" rejected", func(t *testing.T) {
					class := shape.class("flat", nil)
					if class.VectorConfig != nil {
						v := class.VectorConfig["v"]
						v.VectorIndexConfig = nil
						class.VectorConfig["v"] = v
					} else {
						class.VectorIndexConfig = nil
					}
					stored, err := create(t, []string{def}, def, class,
						func(h *Handler) { h.config.DefaultVectorDistanceMetric = metric })
					requireTypeRejection(t, stored, err, def, "flat")
				})
			}
			t.Run(def+"/"+shape.name+"/hnsw default applied", func(t *testing.T) {
				stored, err := create(t, []string{def}, def, shape.class("hnsw", rqIndexConfig("hnsw", "", "cosine", nil)))
				require.NoError(t, err)
				require.NotNil(t, stored)
			})
			t.Run(def+"/"+shape.name+"/dynamic with an allowed flat arm compression", func(t *testing.T) {
				cfg := rqIndexConfig("dynamic", "", "cosine", nil)
				cfg["flat"] = map[string]interface{}{"bq": map[string]interface{}{"enabled": true}}
				stored, err := create(t, []string{def, "bq"}, def, shape.class("dynamic", cfg))
				require.NoError(t, err)
				require.NotNil(t, stored)
			})
			t.Run(def+"/"+shape.name+"/flat with none allowed", func(t *testing.T) {
				stored, err := create(t, []string{def, "none"}, def, shape.class("flat", rqIndexConfig("flat", "", "cosine", nil)))
				require.NoError(t, err)
				require.NotNil(t, stored)
			})
			t.Run(def+"/"+shape.name+"/flat unrestricted", func(t *testing.T) {
				stored, err := create(t, nil, def, shape.class("flat", rqIndexConfig("flat", "", "cosine", nil)))
				require.NoError(t, err)
				require.NotNil(t, stored)
			})
		}
	}

	// rq-4 on a dynamic index on hamming fails on both arms for different
	// reasons; the hnsw arm is reported first and its cause is the distance.
	t.Run("rq-4/dynamic/hamming reports the hnsw arm's distance", func(t *testing.T) {
		stored, err := create(t, []string{"rq-4"}, "rq-4", legacyRQClass("dynamic", rqIndexConfig("dynamic", "", "hamming", nil)))
		require.Error(t, err)
		require.Nil(t, stored)
		require.Contains(t, err.Error(), `on the dynamic index's hnsw arm, and the default compression "rq-4" cannot compress distance "hamming"`)
	})
}
