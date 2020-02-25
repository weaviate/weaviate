//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

// Code generated by go-swagger; DO NOT EDIT.

package models

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/swag"
)

// PhoneNumber phone number
// swagger:model PhoneNumber
type PhoneNumber struct {

	// Read-only. The numerical country code (e.g. 49)
	CountryCode uint64 `json:"countryCode,omitempty"`

	// Optional. The ISO 3166-1 alpha-2 country code. This is used to figure out the correct countryCode and international format if only a national number (e.g. 0123 4567) is provided
	DefaultCountry string `json:"defaultCountry,omitempty"`

	// The raw input as the phone number is present in your raw data set. It will be parsed into the standardized formats if valid.
	Input string `json:"input,omitempty"`

	// Read-only. Parsed result in the international format (e.g. +49 123 ...)
	InternationalFormatted string `json:"internationalFormatted,omitempty"`

	// Read-only. The numerical representation of the national part
	National uint64 `json:"national,omitempty"`

	// Read-only. Parsed result in the national format (e.g. 0123 456789)
	NationalFormatted string `json:"nationalFormatted,omitempty"`

	// Read-only. Indicates whether the parsed number is a valid phone number
	Valid bool `json:"valid,omitempty"`
}

// Validate validates this phone number
func (m *PhoneNumber) Validate(formats strfmt.Registry) error {
	return nil
}

// MarshalBinary interface implementation
func (m *PhoneNumber) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *PhoneNumber) UnmarshalBinary(b []byte) error {
	var res PhoneNumber
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
