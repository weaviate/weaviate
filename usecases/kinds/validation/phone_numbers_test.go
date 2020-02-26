package validation

import (
	"context"
	"errors"
	"testing"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/stretchr/testify/assert"
)

func TestPropertyOfTypePhoneNumberValidation(t *testing.T) {

	type test struct {
		name        string
		phone       interface{} // "phone" property in schema
		expectedErr error
	}

	tests := []test{
		test{
			name:  "phone map missing all keys",
			phone: map[string]interface{}{},
			expectedErr: errors.New("invalid phoneNumber property 'phone' on class 'Person': " +
				"phoneNumber is missing required field 'input'"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := &config.WeaviateConfig{}
			validator := New(testSchema(), fakeExists, &fakePeerLister{}, config)

			obj := &models.Thing{
				Class: "Person",
				Schema: map[string]interface{}{
					"phone": map[string]interface{}{},
				},
			}
			err := validator.properties(context.Background(), kind.Thing, obj)
			assert.Equal(t, test.expectedErr, err)
		})
	}
}
