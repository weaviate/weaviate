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
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/stretchr/testify/assert"
)

func Test_Composer(t *testing.T) {
	m1 := &mockMigrator{}
	m2 := &mockMigrator{}

	composer := New(m1, m2)

	t.Run("adding a class", func(t *testing.T) {
		ctx := context.Background()
		class := &models.Class{Class: "Foo"}
		kind := kind.Thing

		t.Run("no errors", func(t *testing.T) {
			m1.On("AddClass", ctx, kind, class).Return(nil).Once()
			m2.On("AddClass", ctx, kind, class).Return(nil).Once()

			err := composer.AddClass(ctx, kind, class)

			assert.Nil(t, err)
			m1.AssertExpectations(t)
			m2.AssertExpectations(t)
		})

		t.Run("one of the two errors", func(t *testing.T) {
			m1.On("AddClass", ctx, kind, class).Return(errors.New("m1 errord")).Once()
			m2.On("AddClass", ctx, kind, class).Return(nil).Once()

			err := composer.AddClass(ctx, kind, class)

			assert.Equal(t, errors.New("migrator composer: m1 errord"), err)
			m1.AssertExpectations(t)
			m2.AssertExpectations(t)
		})

		t.Run("both error", func(t *testing.T) {
			m1.On("AddClass", ctx, kind, class).Return(errors.New("m1 errord")).Once()
			m2.On("AddClass", ctx, kind, class).Return(errors.New("m2 errord")).Once()

			err := composer.AddClass(ctx, kind, class)

			assert.Equal(t, errors.New("migrator composer: m1 errord, m2 errord"), err)
			m1.AssertExpectations(t)
			m2.AssertExpectations(t)
		})
	})

	t.Run("dropping a class", func(t *testing.T) {
		ctx := context.Background()
		class := "Foo"
		kind := kind.Thing

		t.Run("no errors", func(t *testing.T) {
			m1.On("DropClass", ctx, kind, class).Return(nil).Once()
			m2.On("DropClass", ctx, kind, class).Return(nil).Once()

			err := composer.DropClass(ctx, kind, class)

			assert.Nil(t, err)
			m1.AssertExpectations(t)
			m2.AssertExpectations(t)
		})

		t.Run("one of the two errors", func(t *testing.T) {
			m1.On("DropClass", ctx, kind, class).Return(errors.New("m1 errord")).Once()
			m2.On("DropClass", ctx, kind, class).Return(nil).Once()

			err := composer.DropClass(ctx, kind, class)

			assert.Equal(t, errors.New("migrator composer: m1 errord"), err)
			m1.AssertExpectations(t)
			m2.AssertExpectations(t)
		})

		t.Run("both error", func(t *testing.T) {
			m1.On("DropClass", ctx, kind, class).Return(errors.New("m1 errord")).Once()
			m2.On("DropClass", ctx, kind, class).Return(errors.New("m2 errord")).Once()

			err := composer.DropClass(ctx, kind, class)

			assert.Equal(t, errors.New("migrator composer: m1 errord, m2 errord"), err)
			m1.AssertExpectations(t)
			m2.AssertExpectations(t)
		})
	})

	t.Run("updating a class", func(t *testing.T) {
		ctx := context.Background()
		kind := kind.Thing
		class := "Foo"
		newName := "Bar"
		newKeywords := &models.Keywords{}

		t.Run("no errors", func(t *testing.T) {
			m1.On("UpdateClass", ctx, kind, class, &newName, newKeywords).
				Return(nil).Once()
			m2.On("UpdateClass", ctx, kind, class, &newName, newKeywords).
				Return(nil).Once()

			err := composer.UpdateClass(ctx, kind, class, &newName, newKeywords)

			assert.Nil(t, err)
			m1.AssertExpectations(t)
			m2.AssertExpectations(t)
		})

		t.Run("one of the two errors", func(t *testing.T) {
			m1.On("UpdateClass", ctx, kind, class, &newName, newKeywords).
				Return(errors.New("m1 errord")).Once()
			m2.On("UpdateClass", ctx, kind, class, &newName, newKeywords).
				Return(nil).Once()

			err := composer.UpdateClass(ctx, kind, class, &newName, newKeywords)

			assert.Equal(t, errors.New("migrator composer: m1 errord"), err)
			m1.AssertExpectations(t)
			m2.AssertExpectations(t)
		})

		t.Run("both error", func(t *testing.T) {
			m1.On("UpdateClass", ctx, kind, class, &newName, newKeywords).
				Return(errors.New("m1 errord")).Once()
			m2.On("UpdateClass", ctx, kind, class, &newName, newKeywords).
				Return(errors.New("m2 errord")).Once()

			err := composer.UpdateClass(ctx, kind, class, &newName, newKeywords)

			assert.Equal(t, errors.New("migrator composer: m1 errord, m2 errord"), err)
			m1.AssertExpectations(t)
			m2.AssertExpectations(t)
		})
	})

	t.Run("adding a property", func(t *testing.T) {
		ctx := context.Background()
		class := "Foo"
		kind := kind.Thing
		prop := &models.Property{Name: "Prop"}

		t.Run("no errors", func(t *testing.T) {
			m1.On("AddProperty", ctx, kind, class, prop).Return(nil).Once()
			m2.On("AddProperty", ctx, kind, class, prop).Return(nil).Once()

			err := composer.AddProperty(ctx, kind, class, prop)

			assert.Nil(t, err)
			m1.AssertExpectations(t)
			m2.AssertExpectations(t)
		})

		t.Run("one of the two errors", func(t *testing.T) {
			m1.On("AddProperty", ctx, kind, class, prop).
				Return(errors.New("m1 errord")).Once()
			m2.On("AddProperty", ctx, kind, class, prop).Return(nil).Once()

			err := composer.AddProperty(ctx, kind, class, prop)

			assert.Equal(t, errors.New("migrator composer: m1 errord"), err)
			m1.AssertExpectations(t)
			m2.AssertExpectations(t)
		})

		t.Run("both error", func(t *testing.T) {
			m1.On("AddProperty", ctx, kind, class, prop).
				Return(errors.New("m1 errord")).Once()
			m2.On("AddProperty", ctx, kind, class, prop).
				Return(errors.New("m2 errord")).Once()

			err := composer.AddProperty(ctx, kind, class, prop)

			assert.Equal(t, errors.New("migrator composer: m1 errord, m2 errord"), err)
			m1.AssertExpectations(t)
			m2.AssertExpectations(t)
		})
	})

	t.Run("dropping a property", func(t *testing.T) {
		ctx := context.Background()
		class := "Foo"
		kind := kind.Thing
		prop := "someProp"

		t.Run("no errors", func(t *testing.T) {
			m1.On("DropProperty", ctx, kind, class, prop).Return(nil).Once()
			m2.On("DropProperty", ctx, kind, class, prop).Return(nil).Once()

			err := composer.DropProperty(ctx, kind, class, prop)

			assert.Nil(t, err)
			m1.AssertExpectations(t)
			m2.AssertExpectations(t)
		})

		t.Run("one of the two errors", func(t *testing.T) {
			m1.On("DropProperty", ctx, kind, class, prop).
				Return(errors.New("m1 errord")).Once()
			m2.On("DropProperty", ctx, kind, class, prop).Return(nil).Once()

			err := composer.DropProperty(ctx, kind, class, prop)

			assert.Equal(t, errors.New("migrator composer: m1 errord"), err)
			m1.AssertExpectations(t)
			m2.AssertExpectations(t)
		})

		t.Run("both error", func(t *testing.T) {
			m1.On("DropProperty", ctx, kind, class, prop).
				Return(errors.New("m1 errord")).Once()
			m2.On("DropProperty", ctx, kind, class, prop).
				Return(errors.New("m2 errord")).Once()

			err := composer.DropProperty(ctx, kind, class, prop)

			assert.Equal(t, errors.New("migrator composer: m1 errord, m2 errord"), err)
			m1.AssertExpectations(t)
			m2.AssertExpectations(t)
		})
	})

	t.Run("updating a property", func(t *testing.T) {
		ctx := context.Background()
		kind := kind.Thing
		class := "Foo"
		prop := "someProp"
		newName := "otherProp"
		newKeywords := &models.Keywords{}

		t.Run("no errors", func(t *testing.T) {
			m1.On("UpdateProperty", ctx, kind, class, prop, &newName, newKeywords).
				Return(nil).Once()
			m2.On("UpdateProperty", ctx, kind, class, prop, &newName, newKeywords).
				Return(nil).Once()

			err := composer.UpdateProperty(ctx, kind, class, prop, &newName, newKeywords)

			assert.Nil(t, err)
			m1.AssertExpectations(t)
			m2.AssertExpectations(t)
		})

		t.Run("one of the two errors", func(t *testing.T) {
			m1.On("UpdateProperty", ctx, kind, class, prop, &newName, newKeywords).
				Return(errors.New("m1 errord")).Once()
			m2.On("UpdateProperty", ctx, kind, class, prop, &newName, newKeywords).
				Return(nil).Once()

			err := composer.UpdateProperty(ctx, kind, class, prop, &newName, newKeywords)

			assert.Equal(t, errors.New("migrator composer: m1 errord"), err)
			m1.AssertExpectations(t)
			m2.AssertExpectations(t)
		})

		t.Run("both error", func(t *testing.T) {
			m1.On("UpdateProperty", ctx, kind, class, prop, &newName, newKeywords).
				Return(errors.New("m1 errord")).Once()
			m2.On("UpdateProperty", ctx, kind, class, prop, &newName, newKeywords).
				Return(errors.New("m2 errord")).Once()

			err := composer.UpdateProperty(ctx, kind, class, prop, &newName, newKeywords)

			assert.Equal(t, errors.New("migrator composer: m1 errord, m2 errord"), err)
			m1.AssertExpectations(t)
			m2.AssertExpectations(t)
		})
	})

	t.Run("updating a property, extending the data type list", func(t *testing.T) {
		ctx := context.Background()
		kind := kind.Thing
		class := "Foo"
		prop := "someProp"
		dataType := "newDataType"

		t.Run("no errors", func(t *testing.T) {
			m1.On("UpdatePropertyAddDataType", ctx, kind, class, prop, dataType).
				Return(nil).Once()
			m2.On("UpdatePropertyAddDataType", ctx, kind, class, prop, dataType).
				Return(nil).Once()

			err := composer.UpdatePropertyAddDataType(ctx, kind, class, prop, dataType)

			assert.Nil(t, err)
			m1.AssertExpectations(t)
			m2.AssertExpectations(t)
		})

		t.Run("one of the two errors", func(t *testing.T) {
			m1.On("UpdatePropertyAddDataType", ctx, kind, class, prop, dataType).
				Return(errors.New("m1 errord")).Once()
			m2.On("UpdatePropertyAddDataType", ctx, kind, class, prop, dataType).
				Return(nil).Once()

			err := composer.UpdatePropertyAddDataType(ctx, kind, class, prop, dataType)

			assert.Equal(t, errors.New("migrator composer: m1 errord"), err)
			m1.AssertExpectations(t)
			m2.AssertExpectations(t)
		})

		t.Run("both error", func(t *testing.T) {
			m1.On("UpdatePropertyAddDataType", ctx, kind, class, prop, dataType).
				Return(errors.New("m1 errord")).Once()
			m2.On("UpdatePropertyAddDataType", ctx, kind, class, prop, dataType).
				Return(errors.New("m2 errord")).Once()

			err := composer.UpdatePropertyAddDataType(ctx, kind, class, prop, dataType)

			assert.Equal(t, errors.New("migrator composer: m1 errord, m2 errord"), err)
			m1.AssertExpectations(t)
			m2.AssertExpectations(t)
		})
	})
}
