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

package validation

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestPropertyOfTypePhoneNumberValidation(t *testing.T) {
	type test struct {
		name           string
		phone          interface{} // "phone" property in schema
		expectedErr    error
		expectedResult *models.PhoneNumber
	}

	tests := []test{
		{
			name:  "phone of wrong type",
			phone: "how about a string",
			expectedErr: errors.New("invalid phoneNumber property 'phone' on class 'Person': " +
				"phoneNumber must be a map, but got: string"),
		},
		{
			name:  "phone map missing all keys",
			phone: map[string]interface{}{},
			expectedErr: errors.New("invalid phoneNumber property 'phone' on class 'Person': " +
				"phoneNumber is missing required field 'input'"),
		},
		{
			name: "input is not a string",
			phone: map[string]interface{}{
				"input": 1234,
			},
			expectedErr: errors.New("invalid phoneNumber property 'phone' on class 'Person': " +
				"phoneNumber.input must be a string"),
		},
		{
			name: "default country is not a string",
			phone: map[string]interface{}{
				"input":          "1234",
				"defaultCountry": 7,
			},
			expectedErr: errors.New("invalid phoneNumber property 'phone' on class 'Person': " +
				"phoneNumber.defaultCountry must be a string"),
		},
		{
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
		{
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
		{
			name: "with national number, but missing defaultCountry",
			phone: map[string]interface{}{
				"input": "01711234567",
			},
			expectedErr: fmt.Errorf("invalid phoneNumber property 'phone' on class 'Person': " +
				"invalid phone number: invalid or missing defaultCountry - " +
				"this field is optional if the specified number is in the international format, " +
				"but required if the number is in national format, use ISO 3166-1 alpha-2"),
		},
		{
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
		{
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
		{
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
			validator := New(fakeExists, config, nil)

			obj := &models.Object{
				Class: "Person",
				Properties: map[string]interface{}{
					"phone": test.phone,
				},
			}
			schema := testSchema()
			err := validator.properties(context.Background(), schema.Objects.Classes[0], obj, nil)
			assert.Equal(t, test.expectedErr, err)
			if err != nil {
				return
			}
			phone, ok := obj.Properties.(map[string]interface{})["phone"]
			require.True(t, ok)
			assert.Equal(t, test.expectedResult, phone)
		})
	}
}
