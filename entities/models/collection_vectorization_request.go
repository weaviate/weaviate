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
	"github.com/go-openapi/swag"
)

// CollectionVectorizationRequest Request body for vectorizing target vector of a collection
//
// swagger:model CollectionVectorizationRequest
type CollectionVectorizationRequest struct {

	// Optional filter to scope the vectorization to a subset of tenants
	TenantFilter *string `json:"tenantFilter,omitempty"`
}

// Validate validates this collection vectorization request
func (m *CollectionVectorizationRequest) Validate(formats strfmt.Registry) error {
	return nil
}

// ContextValidate validates this collection vectorization request based on context it is used
func (m *CollectionVectorizationRequest) ContextValidate(ctx context.Context, formats strfmt.Registry) error {
	return nil
}

// MarshalBinary interface implementation
func (m *CollectionVectorizationRequest) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *CollectionVectorizationRequest) UnmarshalBinary(b []byte) error {
	var res CollectionVectorizationRequest
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
