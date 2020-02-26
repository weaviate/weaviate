package validation

import (
	"fmt"
	"strings"

	"github.com/nyaruka/phonenumbers"
	"github.com/semi-technologies/weaviate/entities/models"
)

func parsePhoneNumber(input, defaultCountry string) (*models.PhoneNumber, error) {
	defaultCountry = strings.ToUpper(defaultCountry)
	num, err := phonenumbers.Parse(input, defaultCountry)
	if err != nil {
		return nil, fmt.Errorf("invalid phone number: %v", err)
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
