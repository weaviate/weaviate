//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package modcentroid

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestRef2VecCentroid(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	sp := newFakeStorageProvider(t)
	logger, _ := test.NewNullLogger()
	params := moduletools.NewInitParams(sp, nil, config.Config{}, logger)

	mod := New()
	classConfig := fakeClassConfig(mod.ClassConfigDefaults())
	refProp := "someRef"
	classConfig["referenceProperties"] = []interface{}{refProp}

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

	t.Run("MetaInfo", func(t *testing.T) {
		meta, err := mod.MetaInfo()
		assert.Nil(t, err)
		assert.Empty(t, meta)
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
				"validate %q: invalid config: must have at least one "+
					"value in the \"referenceProperties\" field",
				class.Class)
			err := mod.ValidateClass(ctx, class, cfg)
			assert.EqualError(t, err, expectedErr)
		})
	})

	t.Run("VectorizeObject", func(t *testing.T) {
		t.Run("expected success", func(t *testing.T) {
			t.Run("one refVec", func(t *testing.T) {
				repo := &fakeObjectsRepo{}
				ref := crossref.New("localhost", "SomeClass", strfmt.UUID(uuid.NewString()))
				obj := &models.Object{Properties: map[string]interface{}{
					refProp: models.MultipleRef{ref.SingleRef()},
				}}

				repo.On("Object", ctx, ref.Class, ref.TargetID).
					Return(&search.Result{Vector: []float32{1, 2, 3}}, nil)

				err := mod.VectorizeObject(ctx, obj, classConfig, repo.Object)
				assert.Nil(t, err)
				expectedVec := models.C11yVector{1, 2, 3}
				assert.EqualValues(t, expectedVec, obj.Vector)
			})

			t.Run("no refVecs", func(t *testing.T) {
				repo := &fakeObjectsRepo{}
				ref := crossref.New("localhost", "SomeClass", strfmt.UUID(uuid.NewString()))
				obj := &models.Object{Properties: map[string]interface{}{
					refProp: models.MultipleRef{ref.SingleRef()},
				}}

				repo.On("Object", ctx, ref.Class, ref.TargetID).
					Return(&search.Result{}, nil)

				err := mod.VectorizeObject(ctx, obj, classConfig, repo.Object)
				assert.Nil(t, err)
				assert.Nil(t, nil, obj.Vector)
			})
		})

		t.Run("expected error", func(t *testing.T) {
			t.Run("mismatched refVec lengths", func(t *testing.T) {
				repo := &fakeObjectsRepo{}
				ref1 := crossref.New("localhost", "SomeClass", strfmt.UUID(uuid.NewString()))
				ref2 := crossref.New("localhost", "OtherClass", strfmt.UUID(uuid.NewString()))
				obj := &models.Object{Properties: map[string]interface{}{
					refProp: models.MultipleRef{
						ref1.SingleRef(),
						ref2.SingleRef(),
					},
				}}
				expectedErr := fmt.Errorf("calculate vector: calculate mean: " +
					"found vectors of different length: 2 and 3")

				repo.On("Object", ctx, ref1.Class, ref1.TargetID).
					Return(&search.Result{Vector: []float32{1, 2}}, nil)
				repo.On("Object", ctx, ref2.Class, ref2.TargetID).
					Return(&search.Result{Vector: []float32{1, 2, 3}}, nil)

				err := mod.VectorizeObject(ctx, obj, classConfig, repo.Object)
				assert.EqualError(t, err, expectedErr.Error())
			})
		})
	})
}

type fakeObjectsRepo struct {
	mock.Mock
}

func (r *fakeObjectsRepo) Object(ctx context.Context, class string,
	id strfmt.UUID, props search.SelectProperties,
	addl additional.Properties, tenant string,
) (*search.Result, error) {
	args := r.Called(ctx, class, id)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*search.Result), args.Error(1)
}
