// Code generated by go-swagger; DO NOT EDIT.

package models

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"context"

	"github.com/go-openapi/strfmt"
)

// Vector A Vector object
//
// swagger:model Vector
type Vector []float32

// Validate validates this vector
func (m Vector) Validate(formats strfmt.Registry) error {
	return nil
}

// ContextValidate validates this vector based on context it is used
func (m Vector) ContextValidate(ctx context.Context, formats strfmt.Registry) error {
	return nil
}
