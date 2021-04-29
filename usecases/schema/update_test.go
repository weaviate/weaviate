package schema

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// As of now, most class settings are immutable, but we need to allow some
// specific updates, such as the vector index config
func TestClassUpdates(t *testing.T) {
	t.Run("various immutable and mutable fields", func(t *testing.T) {
		type test struct {
			name          string
			initial       *models.Class
			update        *models.Class
			expectedError error
		}

		tests := []test{
			{
				name:    "attempting a name change",
				initial: &models.Class{Class: "InitialName"},
				update:  &models.Class{Class: "UpdatedName"},
				expectedError: errors.Errorf(
					"class name is immutable: " +
						"attempted change from \"InitialName\" to \"UpdatedName\""),
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				sm := newSchemaManager()
				assert.Nil(t, sm.AddObject(context.Background(), nil, test.initial))
				err := sm.UpdateClass(context.Background(), nil, test.initial.Class, test.update)
				if test.expectedError == nil {
					assert.Nil(t, err)
				} else {
					require.NotNil(t, err, "update must error")
					assert.Equal(t, test.expectedError.Error(), err.Error())
				}
			})
		}
	})
}
