//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
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

// Meta Contains meta information of the current Weaviate instance.
//
// swagger:model Meta
type Meta struct {

	// Port of the GRPC endpoint
	GrpcPort int64 `json:"grpcPort,omitempty"`

	// The url of the host.
	Hostname string `json:"hostname,omitempty"`

	// Module-specific meta information
	Modules interface{} `json:"modules,omitempty"`

	// Version of weaviate you are currently running
	Version string `json:"version,omitempty"`
}

// Validate validates this meta
func (m *Meta) Validate(formats strfmt.Registry) error {
	return nil
}

// ContextValidate validates this meta based on context it is used
func (m *Meta) ContextValidate(ctx context.Context, formats strfmt.Registry) error {
	return nil
}

// MarshalBinary interface implementation
func (m *Meta) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *Meta) UnmarshalBinary(b []byte) error {
	var res Meta
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
