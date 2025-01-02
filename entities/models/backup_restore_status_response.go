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

// BackupRestoreStatusResponse The definition of a backup restore metadata
//
// swagger:model BackupRestoreStatusResponse
type BackupRestoreStatusResponse struct {

	// Backup backend name e.g. filesystem, gcs, s3.
	Backend string `json:"backend,omitempty"`

	// error message if restoration failed
	Error string `json:"error,omitempty"`

	// The ID of the backup. Must be URL-safe and work as a filesystem path, only lowercase, numbers, underscore, minus characters allowed.
	ID string `json:"id,omitempty"`

	// destination path of backup files proper to selected backup backend, contains bucket and path
	Path string `json:"path,omitempty"`

	// phase of backup restoration process
	// Enum: [STARTED TRANSFERRING TRANSFERRED SUCCESS FAILED CANCELED]
	Status *string `json:"status,omitempty"`
}

// Validate validates this backup restore status response
func (m *BackupRestoreStatusResponse) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateStatus(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

var backupRestoreStatusResponseTypeStatusPropEnum []interface{}

func init() {
	var res []string
	if err := json.Unmarshal([]byte(`["STARTED","TRANSFERRING","TRANSFERRED","SUCCESS","FAILED","CANCELED"]`), &res); err != nil {
		panic(err)
	}
	for _, v := range res {
		backupRestoreStatusResponseTypeStatusPropEnum = append(backupRestoreStatusResponseTypeStatusPropEnum, v)
	}
}

const (

	// BackupRestoreStatusResponseStatusSTARTED captures enum value "STARTED"
	BackupRestoreStatusResponseStatusSTARTED string = "STARTED"

	// BackupRestoreStatusResponseStatusTRANSFERRING captures enum value "TRANSFERRING"
	BackupRestoreStatusResponseStatusTRANSFERRING string = "TRANSFERRING"

	// BackupRestoreStatusResponseStatusTRANSFERRED captures enum value "TRANSFERRED"
	BackupRestoreStatusResponseStatusTRANSFERRED string = "TRANSFERRED"

	// BackupRestoreStatusResponseStatusSUCCESS captures enum value "SUCCESS"
	BackupRestoreStatusResponseStatusSUCCESS string = "SUCCESS"

	// BackupRestoreStatusResponseStatusFAILED captures enum value "FAILED"
	BackupRestoreStatusResponseStatusFAILED string = "FAILED"

	// BackupRestoreStatusResponseStatusCANCELED captures enum value "CANCELED"
	BackupRestoreStatusResponseStatusCANCELED string = "CANCELED"
)

// prop value enum
func (m *BackupRestoreStatusResponse) validateStatusEnum(path, location string, value string) error {
	if err := validate.EnumCase(path, location, value, backupRestoreStatusResponseTypeStatusPropEnum, true); err != nil {
		return err
	}
	return nil
}

func (m *BackupRestoreStatusResponse) validateStatus(formats strfmt.Registry) error {
	if swag.IsZero(m.Status) { // not required
		return nil
	}

	// value enum
	if err := m.validateStatusEnum("status", "body", *m.Status); err != nil {
		return err
	}

	return nil
}

// ContextValidate validates this backup restore status response based on context it is used
func (m *BackupRestoreStatusResponse) ContextValidate(ctx context.Context, formats strfmt.Registry) error {
	return nil
}

// MarshalBinary interface implementation
func (m *BackupRestoreStatusResponse) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *BackupRestoreStatusResponse) UnmarshalBinary(b []byte) error {
	var res BackupRestoreStatusResponse
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
