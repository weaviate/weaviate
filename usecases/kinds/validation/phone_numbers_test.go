package validation

import (
	"context"
	"errors"
	"testing"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPropertyOfTypePhoneNumberValidation(t *testing.T) {

	type test struct {
		name           string
		phone          interface{} // "phone" property in schema
		expectedErr    error
		expectedResult *models.PhoneNumber
	}

	tests := []test{
		test{
			name:  "phone of wrong type",
			phone: "how about a string",
			expectedErr: errors.New("invalid phoneNumber property 'phone' on class 'Person': " +
				"phoneNumber must be a map, but got: string"),
		},
		test{
			name:  "phone map missing all keys",
			phone: map[string]interface{}{},
			expectedErr: errors.New("invalid phoneNumber property 'phone' on class 'Person': " +
				"phoneNumber is missing required field 'input'"),
		},
		test{
			name: "with only input set",
			phone: map[string]interface{}{
				"input": "+491711234567",
			},
			expectedErr: nil,
			expectedResult: &models.PhoneNumber{
				Valid:                  true,
				Input:                  "+491711234567",
				InternationalFormatted: "+49 171 1234567",
				CountryCode:            49,
				National:               1711234567,
				NationalFormatted:      "0171 1234567",
			},
		},
		test{
			name: "with national number and country uppercased",
			phone: map[string]interface{}{
				"input":          "01711234567",
				"defaultCountry": "DE",
			},
			expectedErr: nil,
			expectedResult: &models.PhoneNumber{
				Valid:                  true,
				DefaultCountry:         "DE",
				Input:                  "01711234567",
				InternationalFormatted: "+49 171 1234567",
				CountryCode:            49,
				National:               1711234567,
				NationalFormatted:      "0171 1234567",
			},
		},
		test{
			name: "with national number and country uppercased",
			phone: map[string]interface{}{
				"input":          "01711234567",
				"defaultCountry": "de",
			},
			expectedErr: nil,
			expectedResult: &models.PhoneNumber{
				Valid:                  true,
				DefaultCountry:         "DE",
				Input:                  "01711234567",
				InternationalFormatted: "+49 171 1234567",
				CountryCode:            49,
				National:               1711234567,
				NationalFormatted:      "0171 1234567",
			},
		},
		test{
			name: "with national number and various special characters",
			phone: map[string]interface{}{
				"input":          "(0)171-123 456 7",
				"defaultCountry": "de",
			},
			expectedErr: nil,
			expectedResult: &models.PhoneNumber{
				Valid:                  true,
				DefaultCountry:         "DE",
				Input:                  "(0)171-123 456 7",
				InternationalFormatted: "+49 171 1234567",
				CountryCode:            49,
				National:               1711234567,
				NationalFormatted:      "0171 1234567",
			},
		},
		test{
			name: "with international number and optional zero after country code",
			phone: map[string]interface{}{
				"input":          "+49 (0) 171 123 456 7",
				"defaultCountry": "de",
			},
			expectedErr: nil,
			expectedResult: &models.PhoneNumber{
				Valid:                  true,
				DefaultCountry:         "DE",
				Input:                  "+49 (0) 171 123 456 7",
				InternationalFormatted: "+49 171 1234567",
				CountryCode:            49,
				National:               1711234567,
				NationalFormatted:      "0171 1234567",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := &config.WeaviateConfig{}
			validator := New(testSchema(), fakeExists, &fakePeerLister{}, config)

			obj := &models.Thing{
				Class: "Person",
				Schema: map[string]interface{}{
					"phone": test.phone,
				},
			}
			err := validator.properties(context.Background(), kind.Thing, obj)
			assert.Equal(t, test.expectedErr, err)
			if err != nil {
				return
			}
			phone, ok := obj.Schema.(map[string]interface{})["phone"]
			require.True(t, ok)
			assert.Equal(t, test.expectedResult, phone)
		})
	}
}
