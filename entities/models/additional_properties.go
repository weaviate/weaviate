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

// Code generated by go-swagger; DO NOT EDIT.

package models

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"context"

	"github.com/go-openapi/strfmt"
)

// AdditionalProperties (Response only) Additional meta information about a single object.
//
// swagger:model AdditionalProperties
type AdditionalProperties map[string]interface{}

// Validate validates this additional properties
func (m AdditionalProperties) Validate(formats strfmt.Registry) error {
	return nil
}

// ContextValidate validates this additional properties based on context it is used
func (m AdditionalProperties) ContextValidate(ctx context.Context, formats strfmt.Registry) error {
	return nil
}
