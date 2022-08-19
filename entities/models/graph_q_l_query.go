// Code generated by go-swagger; DO NOT EDIT.

package models

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
)

// GraphQLQuery GraphQL query based on: http://facebook.github.io/graphql/.
//
// swagger:model GraphQLQuery
type GraphQLQuery struct {

	// The name of the operation if multiple exist in the query.
	OperationName string `json:"operationName,omitempty"`

	// Query based on GraphQL syntax.
	Query string `json:"query,omitempty"`

	// Additional variables for the query.
	Variables interface{} `json:"variables,omitempty"`
}

// Validate validates this graph q l query
func (m *GraphQLQuery) Validate(formats strfmt.Registry) error {
	return nil
}

// MarshalBinary interface implementation
func (m *GraphQLQuery) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *GraphQLQuery) UnmarshalBinary(b []byte) error {
	var res GraphQLQuery
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
