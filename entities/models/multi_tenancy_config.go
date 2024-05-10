// Code generated by go-swagger; DO NOT EDIT.

package models

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"context"

	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
)

// MultiTenancyConfig Configuration related to multi-tenancy within a class
//
// swagger:model MultiTenancyConfig
type MultiTenancyConfig struct {

	// Existing tenants should (not) be turned HOT implicitly when they are accessed and in another activity status
	AutoTenantActivation bool `json:"autoTenantActivation"`

	// Nonexistent tenants should (not) be created implicitly
	AutoTenantCreation bool `json:"autoTenantCreation"`

	// Whether or not multi-tenancy is enabled for this class
	Enabled bool `json:"enabled"`
}

// Validate validates this multi tenancy config
func (m *MultiTenancyConfig) Validate(formats strfmt.Registry) error {
	return nil
}

// ContextValidate validates this multi tenancy config based on context it is used
func (m *MultiTenancyConfig) ContextValidate(ctx context.Context, formats strfmt.Registry) error {
	return nil
}

// MarshalBinary interface implementation
func (m *MultiTenancyConfig) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *MultiTenancyConfig) UnmarshalBinary(b []byte) error {
	var res MultiTenancyConfig
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
