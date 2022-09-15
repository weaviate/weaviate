package modcentroid

import (
	"context"
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/moduletools"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
)

func TestRef2VecCentroid(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	sp := newFakeStorageProvider(t)
	logger, _ := test.NewNullLogger()
	params := moduletools.NewInitParams(sp, nil, logger)

	mod := New()
	cfg := fakeClassConfig(mod.ClassConfigDefaults())
	refProp := "someRef"
	cfg[referencePropertiesField] = []interface{}{refProp}

	t.Run("Init", func(t *testing.T) {
		err := mod.Init(ctx, params)
		assert.Nil(t, err)
	})

	t.Run("RootHandler", func(t *testing.T) {
		h := mod.RootHandler()
		assert.Nil(t, h)
	})

	t.Run("Type", func(t *testing.T) {
		typ := mod.Type()
		assert.Equal(t, modulecapabilities.Ref2Vec, typ)
	})

	t.Run("VectorizeObject", func(t *testing.T) {
		obj := &models.Object{}
		refVec := []float32{1, 2, 3}

		err := mod.VectorizeObject(ctx, obj, cfg, refVec)
		assert.Nil(t, err)
		assert.EqualValues(t, refVec, obj.Vector)
	})

	t.Run("TargetReferenceProperties", func(t *testing.T) {
		props := mod.TargetReferenceProperties(cfg)
		expectedProps := []string{refProp}
		assert.EqualValues(t, expectedProps, props)
	})
}
