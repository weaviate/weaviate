package modcentroid

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/moduletools"
	"github.com/semi-technologies/weaviate/entities/schema"
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

	t.Run("Name", func(t *testing.T) {
		name := mod.Name()
		assert.Equal(t, Name, name)
	})

	t.Run("PropertyConfigDefaults", func(t *testing.T) {
		dt := schema.DataType("dataType")
		props := mod.PropertyConfigDefaults(&dt)
		assert.Nil(t, props)
	})

	t.Run("ValidateClass", func(t *testing.T) {
		t.Run("expected success", func(t *testing.T) {
			class := &models.Class{}

			err := mod.ValidateClass(ctx, class, cfg)
			assert.Nil(t, err)
		})

		t.Run("expected error", func(t *testing.T) {
			class := &models.Class{Class: "InvalidConfigClass"}
			cfg := fakeClassConfig{}

			expectedErr := fmt.Sprintf(
				"invalid config: must have at least one value in the %q field for class %q",
				referencePropertiesField, class.Class)
			err := mod.ValidateClass(ctx, class, cfg)
			assert.EqualError(t, err, expectedErr)
		})
	})

	t.Run("VectorizeObject", func(t *testing.T) {
		t.Run("expected success", func(t *testing.T) {
			t.Run("one refVec", func(t *testing.T) {
				obj := &models.Object{}
				refVec := []float32{1, 2, 3}

				err := mod.VectorizeObject(ctx, obj, cfg, refVec)
				assert.Nil(t, err)
				assert.EqualValues(t, refVec, obj.Vector)
			})

			t.Run("no refVecs", func(t *testing.T) {
				obj := &models.Object{}

				err := mod.VectorizeObject(ctx, obj, cfg)
				assert.Nil(t, err)
				assert.Nil(t, nil, obj.Vector)
			})
		})

		t.Run("expected error", func(t *testing.T) {
			t.Run("mismatched refVec lengths", func(t *testing.T) {
				obj := &models.Object{}
				refVecs := [][]float32{{1, 2, 3}, {1, 2}}

				expectedErr := "calculate vector: calculate mean: found vectors of different length: 3 and 2"
				err := mod.VectorizeObject(ctx, obj, cfg, refVecs...)
				assert.EqualError(t, err, expectedErr)
			})
		})
	})

	t.Run("TargetReferenceProperties", func(t *testing.T) {
		props := mod.TargetReferenceProperties(cfg)
		expectedProps := []string{refProp}
		assert.EqualValues(t, expectedProps, props)
	})
}
