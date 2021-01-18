//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package migrate

import (
	"context"
	"errors"
	"testing"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/stretchr/testify/assert"
)

func Test_Composer(t *testing.T) {
	m1 := &mockMigrator{}
	m2 := &mockMigrator{}

	composer := New(m1, m2)

	t.Run("adding a class", func(t *testing.T) {
		ctx := context.Background()
		class := &models.Class{Class: "Foo"}

		t.Run("no errors", func(t *testing.T) {
			m1.On("AddClass", ctx, class).Return(nil).Once()
			m2.On("AddClass", ctx, class).Return(nil).Once()

			err := composer.AddClass(ctx, class)

			assert.Nil(t, err)
			m1.AssertExpectations(t)
			m2.AssertExpectations(t)
		})

		t.Run("one of the two errors", func(t *testing.T) {
			m1.On("AddClass", ctx, class).Return(errors.New("m1 errord")).Once()
			m2.On("AddClass", ctx, class).Return(nil).Once()

			err := composer.AddClass(ctx, class)

			assert.Equal(t, errors.New("migrator composer: m1 errord"), err)
			m1.AssertExpectations(t)
			m2.AssertExpectations(t)
		})

		t.Run("both error", func(t *testing.T) {
			m1.On("AddClass", ctx, class).Return(errors.New("m1 errord")).Once()
			m2.On("AddClass", ctx, class).Return(errors.New("m2 errord")).Once()

			err := composer.AddClass(ctx, class)

			assert.Equal(t, errors.New("migrator composer: m1 errord, m2 errord"), err)
			m1.AssertExpectations(t)
			m2.AssertExpectations(t)
		})
	})

	t.Run("dropping a class", func(t *testing.T) {
		ctx := context.Background()
		class := "Foo"

		t.Run("no errors", func(t *testing.T) {
			m1.On("DropClass", ctx, class).Return(nil).Once()
			m2.On("DropClass", ctx, class).Return(nil).Once()

			err := composer.DropClass(ctx, class)

			assert.Nil(t, err)
			m1.AssertExpectations(t)
			m2.AssertExpectations(t)
		})

		t.Run("one of the two errors", func(t *testing.T) {
			m1.On("DropClass", ctx, class).Return(errors.New("m1 errord")).Once()
			m2.On("DropClass", ctx, class).Return(nil).Once()

			err := composer.DropClass(ctx, class)

			assert.Equal(t, errors.New("migrator composer: m1 errord"), err)
			m1.AssertExpectations(t)
			m2.AssertExpectations(t)
		})

		t.Run("both error", func(t *testing.T) {
			m1.On("DropClass", ctx, class).Return(errors.New("m1 errord")).Once()
			m2.On("DropClass", ctx, class).Return(errors.New("m2 errord")).Once()

			err := composer.DropClass(ctx, class)

			assert.Equal(t, errors.New("migrator composer: m1 errord, m2 errord"), err)
			m1.AssertExpectations(t)
			m2.AssertExpectations(t)
		})
	})

	t.Run("updating a class", func(t *testing.T) {
		ctx := context.Background()
		class := "Foo"
		newName := "Bar"

		t.Run("no errors", func(t *testing.T) {
			m1.On("UpdateClass", ctx, class, &newName).
				Return(nil).Once()
			m2.On("UpdateClass", ctx, class, &newName).
				Return(nil).Once()

			err := composer.UpdateClass(ctx, class, &newName)

			assert.Nil(t, err)
			m1.AssertExpectations(t)
			m2.AssertExpectations(t)
		})

		t.Run("one of the two errors", func(t *testing.T) {
			m1.On("UpdateClass", ctx, class, &newName).
				Return(errors.New("m1 errord")).Once()
			m2.On("UpdateClass", ctx, class, &newName).
				Return(nil).Once()

			err := composer.UpdateClass(ctx, class, &newName)

			assert.Equal(t, errors.New("migrator composer: m1 errord"), err)
			m1.AssertExpectations(t)
			m2.AssertExpectations(t)
		})

		t.Run("both error", func(t *testing.T) {
			m1.On("UpdateClass", ctx, class, &newName).
				Return(errors.New("m1 errord")).Once()
			m2.On("UpdateClass", ctx, class, &newName).
				Return(errors.New("m2 errord")).Once()

			err := composer.UpdateClass(ctx, class, &newName)

			assert.Equal(t, errors.New("migrator composer: m1 errord, m2 errord"), err)
			m1.AssertExpectations(t)
			m2.AssertExpectations(t)
		})
	})

	t.Run("adding a property", func(t *testing.T) {
		ctx := context.Background()
		class := "Foo"
		prop := &models.Property{Name: "Prop"}

		t.Run("no errors", func(t *testing.T) {
			m1.On("AddProperty", ctx, class, prop).Return(nil).Once()
			m2.On("AddProperty", ctx, class, prop).Return(nil).Once()

			err := composer.AddProperty(ctx, class, prop)

			assert.Nil(t, err)
			m1.AssertExpectations(t)
			m2.AssertExpectations(t)
		})

		t.Run("one of the two errors", func(t *testing.T) {
			m1.On("AddProperty", ctx, class, prop).
				Return(errors.New("m1 errord")).Once()
			m2.On("AddProperty", ctx, class, prop).Return(nil).Once()

			err := composer.AddProperty(ctx, class, prop)

			assert.Equal(t, errors.New("migrator composer: m1 errord"), err)
			m1.AssertExpectations(t)
			m2.AssertExpectations(t)
		})

		t.Run("both error", func(t *testing.T) {
			m1.On("AddProperty", ctx, class, prop).
				Return(errors.New("m1 errord")).Once()
			m2.On("AddProperty", ctx, class, prop).
				Return(errors.New("m2 errord")).Once()

			err := composer.AddProperty(ctx, class, prop)

			assert.Equal(t, errors.New("migrator composer: m1 errord, m2 errord"), err)
			m1.AssertExpectations(t)
			m2.AssertExpectations(t)
		})
	})

	t.Run("dropping a property", func(t *testing.T) {
		ctx := context.Background()
		class := "Foo"
		prop := "someProp"

		t.Run("no errors", func(t *testing.T) {
			m1.On("DropProperty", ctx, class, prop).Return(nil).Once()
			m2.On("DropProperty", ctx, class, prop).Return(nil).Once()

			err := composer.DropProperty(ctx, class, prop)

			assert.Nil(t, err)
			m1.AssertExpectations(t)
			m2.AssertExpectations(t)
		})

		t.Run("one of the two errors", func(t *testing.T) {
			m1.On("DropProperty", ctx, class, prop).
				Return(errors.New("m1 errord")).Once()
			m2.On("DropProperty", ctx, class, prop).Return(nil).Once()

			err := composer.DropProperty(ctx, class, prop)

			assert.Equal(t, errors.New("migrator composer: m1 errord"), err)
			m1.AssertExpectations(t)
			m2.AssertExpectations(t)
		})

		t.Run("both error", func(t *testing.T) {
			m1.On("DropProperty", ctx, class, prop).
				Return(errors.New("m1 errord")).Once()
			m2.On("DropProperty", ctx, class, prop).
				Return(errors.New("m2 errord")).Once()

			err := composer.DropProperty(ctx, class, prop)

			assert.Equal(t, errors.New("migrator composer: m1 errord, m2 errord"), err)
			m1.AssertExpectations(t)
			m2.AssertExpectations(t)
		})
	})

	t.Run("updating a property", func(t *testing.T) {
		ctx := context.Background()
		class := "Foo"
		prop := "someProp"
		newName := "otherProp"

		t.Run("no errors", func(t *testing.T) {
			m1.On("UpdateProperty", ctx, class, prop, &newName).
				Return(nil).Once()
			m2.On("UpdateProperty", ctx, class, prop, &newName).
				Return(nil).Once()

			err := composer.UpdateProperty(ctx, class, prop, &newName)

			assert.Nil(t, err)
			m1.AssertExpectations(t)
			m2.AssertExpectations(t)
		})

		t.Run("one of the two errors", func(t *testing.T) {
			m1.On("UpdateProperty", ctx, class, prop, &newName).
				Return(errors.New("m1 errord")).Once()
			m2.On("UpdateProperty", ctx, class, prop, &newName).
				Return(nil).Once()

			err := composer.UpdateProperty(ctx, class, prop, &newName)

			assert.Equal(t, errors.New("migrator composer: m1 errord"), err)
			m1.AssertExpectations(t)
			m2.AssertExpectations(t)
		})

		t.Run("both error", func(t *testing.T) {
			m1.On("UpdateProperty", ctx, class, prop, &newName).
				Return(errors.New("m1 errord")).Once()
			m2.On("UpdateProperty", ctx, class, prop, &newName).
				Return(errors.New("m2 errord")).Once()

			err := composer.UpdateProperty(ctx, class, prop, &newName)

			assert.Equal(t, errors.New("migrator composer: m1 errord, m2 errord"), err)
			m1.AssertExpectations(t)
			m2.AssertExpectations(t)
		})
	})

	t.Run("updating a property, extending the data type list", func(t *testing.T) {
		ctx := context.Background()
		class := "Foo"
		prop := "someProp"
		dataType := "newDataType"

		t.Run("no errors", func(t *testing.T) {
			m1.On("UpdatePropertyAddDataType", ctx, class, prop, dataType).
				Return(nil).Once()
			m2.On("UpdatePropertyAddDataType", ctx, class, prop, dataType).
				Return(nil).Once()

			err := composer.UpdatePropertyAddDataType(ctx, class, prop, dataType)

			assert.Nil(t, err)
			m1.AssertExpectations(t)
			m2.AssertExpectations(t)
		})

		t.Run("one of the two errors", func(t *testing.T) {
			m1.On("UpdatePropertyAddDataType", ctx, class, prop, dataType).
				Return(errors.New("m1 errord")).Once()
			m2.On("UpdatePropertyAddDataType", ctx, class, prop, dataType).
				Return(nil).Once()

			err := composer.UpdatePropertyAddDataType(ctx, class, prop, dataType)

			assert.Equal(t, errors.New("migrator composer: m1 errord"), err)
			m1.AssertExpectations(t)
			m2.AssertExpectations(t)
		})

		t.Run("both error", func(t *testing.T) {
			m1.On("UpdatePropertyAddDataType", ctx, class, prop, dataType).
				Return(errors.New("m1 errord")).Once()
			m2.On("UpdatePropertyAddDataType", ctx, class, prop, dataType).
				Return(errors.New("m2 errord")).Once()

			err := composer.UpdatePropertyAddDataType(ctx, class, prop, dataType)

			assert.Equal(t, errors.New("migrator composer: m1 errord, m2 errord"), err)
			m1.AssertExpectations(t)
			m2.AssertExpectations(t)
		})
	})
}
