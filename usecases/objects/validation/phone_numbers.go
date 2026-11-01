//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package validation

import (
	"errors"
	"fmt"
	"strings"

	"github.com/nyaruka/phonenumbers"

	"github.com/weaviate/weaviate/entities/models"
)

func parsePhoneNumber(input, defaultCountry string) (*models.PhoneNumber, error) {
	defaultCountry = strings.ToUpper(defaultCountry)

	// phonenumbers.Parse ignores defaultCountry when the input is already in
	// international format, so an invalid region code (e.g. "ZZ", the library's
	// UNKNOWN_REGION) would otherwise be accepted and stored silently. Reject a
	// non-empty defaultCountry that is not a known ISO 3166-1 alpha-2 region.
	if defaultCountry != "" && phonenumbers.GetCountryCodeForRegion(defaultCountry) == 0 {
		return nil, fmt.Errorf("invalid phone number: invalid defaultCountry %q - use a valid ISO 3166-1 alpha-2 country code", defaultCountry)
	}

	num, err := phonenumbers.Parse(input, defaultCountry)
	if err != nil {
		switch {
		case errors.Is(err, phonenumbers.ErrInvalidCountryCode):
			return nil, fmt.Errorf("invalid phone number: invalid or missing defaultCountry - this field is optional if the specified number is in the international format, but required if the number is in national format, use ISO 3166-1 alpha-2")
		default:
			return nil, fmt.Errorf("invalid phone number: %w", err)
		}
	}

	return &models.PhoneNumber{
		National:               num.GetNationalNumber(),
		NationalFormatted:      phonenumbers.Format(num, phonenumbers.NATIONAL),
		InternationalFormatted: phonenumbers.Format(num, phonenumbers.INTERNATIONAL),
		CountryCode:            uint64(num.GetCountryCode()),
		DefaultCountry:         defaultCountry,
		Input:                  input,
		Valid:                  phonenumbers.IsValidNumber(num),
	}, nil
}
