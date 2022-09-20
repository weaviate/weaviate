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
	"github.com/semi-technologies/weaviate/modules/ref2vec-centroid/vectorizer"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestRef2VecCentroid(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	sp := newFakeStorageProvider(t)
	logger, _ := test.NewNullLogger()
	params := moduletools.NewInitParams(sp, nil, logger)

	mod := New()
	classConfig := fakeClassConfig(mod.ClassConfigDefaults())
	refProp := "someRef"
	classConfig[vectorizer.ReferencePropertiesField] = []interface{}{refProp}
	classSettings := vectorizer.NewClassSettings(classConfig)

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

			err := mod.ValidateClass(ctx, class, classConfig)
			assert.Nil(t, err)
		})

		t.Run("expected error", func(t *testing.T) {
			class := &models.Class{Class: "InvalidConfigClass"}
			cfg := fakeClassConfig{}

			expectedErr := fmt.Sprintf(
				"invalid config: must have at least one value in the %q field for class %q",
				vectorizer.ReferencePropertiesField, class.Class)
			err := mod.ValidateClass(ctx, class, cfg)
			assert.EqualError(t, err, expectedErr)
		})
	})

	t.Run("VectorizeObject", func(t *testing.T) {
		t.Run("expected success", func(t *testing.T) {
			t.Run("one refVec", func(t *testing.T) {
				repo := &fakeRefVecRepo{}
				obj := &models.Object{}

				repo.On("ReferenceVectorSearch",
					ctx, obj, classSettings.ReferenceProperties()).
					Return([][]float32{{1, 2, 3}}, nil)

				err := mod.VectorizeObject(ctx, obj, classConfig, repo.ReferenceVectorSearch)
				assert.Nil(t, err)
				expectedVec := []float32{1, 2, 3}
				assert.EqualValues(t, expectedVec, obj.Vector)
			})

			t.Run("no refVecs", func(t *testing.T) {
				repo := &fakeRefVecRepo{}
				obj := &models.Object{}

				repo.On("ReferenceVectorSearch",
					ctx, obj, classSettings.ReferenceProperties()).
					Return(nil, nil)

				err := mod.VectorizeObject(ctx, obj, classConfig, repo.ReferenceVectorSearch)
				assert.Nil(t, err)
				assert.Nil(t, nil, obj.Vector)
			})
		})

		t.Run("expected error", func(t *testing.T) {
			t.Run("mismatched refVec lengths", func(t *testing.T) {
				repo := &fakeRefVecRepo{}
				obj := &models.Object{}
				expectedErr := fmt.Errorf("calculate vector: " +
					"calculate mean: found vectors of different length: 3 and 2")

				repo.On("ReferenceVectorSearch",
					ctx, obj, classSettings.ReferenceProperties()).
					Return(nil, expectedErr)

				err := mod.VectorizeObject(ctx, obj, classConfig, repo.ReferenceVectorSearch)
				assert.EqualError(t, err, fmt.Sprintf(
					"find ref vectors: %s", expectedErr.Error()))
			})
		})
	})
}

type fakeRefVecRepo struct {
	mock.Mock
}

func (r *fakeRefVecRepo) ReferenceVectorSearch(ctx context.Context, obj *models.Object,
	refProps map[string]struct{},
) ([][]float32, error) {
	args := r.Called(ctx, obj, refProps)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([][]float32), args.Error(1)
}
