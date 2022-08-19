// Code generated by go-swagger; DO NOT EDIT.

package models

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
)

// StopwordConfig fine-grained control over stopword list usage
//
// swagger:model StopwordConfig
type StopwordConfig struct {

	// stopwords to be considered additionally
	Additions []string `json:"additions"`

	// pre-existing list of common words by language
	Preset string `json:"preset,omitempty"`

	// stopwords to be removed from consideration
	Removals []string `json:"removals"`
}

// Validate validates this stopword config
func (m *StopwordConfig) Validate(formats strfmt.Registry) error {
	return nil
}

// MarshalBinary interface implementation
func (m *StopwordConfig) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *StopwordConfig) UnmarshalBinary(b []byte) error {
	var res StopwordConfig
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
