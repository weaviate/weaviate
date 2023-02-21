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
	"encoding/json"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
	"github.com/go-openapi/validate"
)

// PatchDocumentAction Either a JSONPatch document as defined by RFC 6902 (from, op, path, value), or a merge document (RFC 7396).
//
// swagger:model PatchDocumentAction
type PatchDocumentAction struct {
	// A string containing a JSON Pointer value.
	From string `json:"from,omitempty"`

	// merge
	Merge *Object `json:"merge,omitempty"`

	// The operation to be performed.
	// Required: true
	// Enum: [add remove replace move copy test]
	Op *string `json:"op"`

	// A JSON-Pointer.
	// Required: true
	Path *string `json:"path"`

	// The value to be used within the operations.
	Value interface{} `json:"value,omitempty"`
}

// Validate validates this patch document action
func (m *PatchDocumentAction) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateMerge(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateOp(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validatePath(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *PatchDocumentAction) validateMerge(formats strfmt.Registry) error {
	if swag.IsZero(m.Merge) { // not required
		return nil
	}

	if m.Merge != nil {
		if err := m.Merge.Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("merge")
			} else if ce, ok := err.(*errors.CompositeError); ok {
				return ce.ValidateName("merge")
			}
			return err
		}
	}

	return nil
}

var patchDocumentActionTypeOpPropEnum []interface{}

func init() {
	var res []string
	if err := json.Unmarshal([]byte(`["add","remove","replace","move","copy","test"]`), &res); err != nil {
		panic(err)
	}
	for _, v := range res {
		patchDocumentActionTypeOpPropEnum = append(patchDocumentActionTypeOpPropEnum, v)
	}
}

const (

	// PatchDocumentActionOpAdd captures enum value "add"
	PatchDocumentActionOpAdd string = "add"

	// PatchDocumentActionOpRemove captures enum value "remove"
	PatchDocumentActionOpRemove string = "remove"

	// PatchDocumentActionOpReplace captures enum value "replace"
	PatchDocumentActionOpReplace string = "replace"

	// PatchDocumentActionOpMove captures enum value "move"
	PatchDocumentActionOpMove string = "move"

	// PatchDocumentActionOpCopy captures enum value "copy"
	PatchDocumentActionOpCopy string = "copy"

	// PatchDocumentActionOpTest captures enum value "test"
	PatchDocumentActionOpTest string = "test"
)

// prop value enum
func (m *PatchDocumentAction) validateOpEnum(path, location string, value string) error {
	if err := validate.EnumCase(path, location, value, patchDocumentActionTypeOpPropEnum, true); err != nil {
		return err
	}
	return nil
}

func (m *PatchDocumentAction) validateOp(formats strfmt.Registry) error {
	if err := validate.Required("op", "body", m.Op); err != nil {
		return err
	}

	// value enum
	if err := m.validateOpEnum("op", "body", *m.Op); err != nil {
		return err
	}

	return nil
}

func (m *PatchDocumentAction) validatePath(formats strfmt.Registry) error {
	if err := validate.Required("path", "body", m.Path); err != nil {
		return err
	}

	return nil
}

// ContextValidate validate this patch document action based on the context it is used
func (m *PatchDocumentAction) ContextValidate(ctx context.Context, formats strfmt.Registry) error {
	var res []error

	if err := m.contextValidateMerge(ctx, formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *PatchDocumentAction) contextValidateMerge(ctx context.Context, formats strfmt.Registry) error {
	if m.Merge != nil {
		if err := m.Merge.ContextValidate(ctx, formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("merge")
			} else if ce, ok := err.(*errors.CompositeError); ok {
				return ce.ValidateName("merge")
			}
			return err
		}
	}

	return nil
}

// MarshalBinary interface implementation
func (m *PatchDocumentAction) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *PatchDocumentAction) UnmarshalBinary(b []byte) error {
	var res PatchDocumentAction
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
