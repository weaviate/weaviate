// Code generated by go-swagger; DO NOT EDIT.

package models

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"encoding/json"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
	"github.com/go-openapi/validate"
)

// SnapshotMeta The definition of a snapshot metadata
//
// swagger:model SnapshotMeta
type SnapshotMeta struct {

	// error message if creation failed
	Error string `json:"error,omitempty"`

	// The ID of a snapshot. Must be URL-safe and work as a filesystem path, only lowercase, numbers, underscore, minus characters allowed.
	ID string `json:"id,omitempty"`

	// destination path of snapshot files proper to selected storage
	Path string `json:"path,omitempty"`

	// phase of snapshot creation process
	// Enum: [STARTED TRANSFERRING TRANSFERRED SUCCESS FAILED]
	Status *string `json:"status,omitempty"`

	// Storage name e.g. filesystem, gcs, s3.
	StorageName string `json:"storageName,omitempty"`
}

// Validate validates this snapshot meta
func (m *SnapshotMeta) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateStatus(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

var snapshotMetaTypeStatusPropEnum []interface{}

func init() {
	var res []string
	if err := json.Unmarshal([]byte(`["STARTED","TRANSFERRING","TRANSFERRED","SUCCESS","FAILED"]`), &res); err != nil {
		panic(err)
	}
	for _, v := range res {
		snapshotMetaTypeStatusPropEnum = append(snapshotMetaTypeStatusPropEnum, v)
	}
}

const (

	// SnapshotMetaStatusSTARTED captures enum value "STARTED"
	SnapshotMetaStatusSTARTED string = "STARTED"

	// SnapshotMetaStatusTRANSFERRING captures enum value "TRANSFERRING"
	SnapshotMetaStatusTRANSFERRING string = "TRANSFERRING"

	// SnapshotMetaStatusTRANSFERRED captures enum value "TRANSFERRED"
	SnapshotMetaStatusTRANSFERRED string = "TRANSFERRED"

	// SnapshotMetaStatusSUCCESS captures enum value "SUCCESS"
	SnapshotMetaStatusSUCCESS string = "SUCCESS"

	// SnapshotMetaStatusFAILED captures enum value "FAILED"
	SnapshotMetaStatusFAILED string = "FAILED"
)

// prop value enum
func (m *SnapshotMeta) validateStatusEnum(path, location string, value string) error {
	if err := validate.EnumCase(path, location, value, snapshotMetaTypeStatusPropEnum, true); err != nil {
		return err
	}
	return nil
}

func (m *SnapshotMeta) validateStatus(formats strfmt.Registry) error {

	if swag.IsZero(m.Status) { // not required
		return nil
	}

	// value enum
	if err := m.validateStatusEnum("status", "body", *m.Status); err != nil {
		return err
	}

	return nil
}

// MarshalBinary interface implementation
func (m *SnapshotMeta) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *SnapshotMeta) UnmarshalBinary(b []byte) error {
	var res SnapshotMeta
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
