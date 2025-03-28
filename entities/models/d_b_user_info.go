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
	"encoding/json"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
	"github.com/go-openapi/validate"
)

// DBUserInfo d b user info
//
// swagger:model DBUserInfo
type DBUserInfo struct {

	// activity status of the returned user
	// Required: true
	Active *bool `json:"active"`

	// type of the returned user
	// Required: true
	// Enum: [db_user db_env_user]
	DbUserType *string `json:"dbUserType"`

	// The role names associated to the user
	// Required: true
	Roles []string `json:"roles"`

	// The user id of the given user
	// Required: true
	UserID *string `json:"userId"`
}

// Validate validates this d b user info
func (m *DBUserInfo) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateActive(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateDbUserType(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateRoles(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateUserID(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *DBUserInfo) validateActive(formats strfmt.Registry) error {

	if err := validate.Required("active", "body", m.Active); err != nil {
		return err
	}

	return nil
}

var dBUserInfoTypeDbUserTypePropEnum []interface{}

func init() {
	var res []string
	if err := json.Unmarshal([]byte(`["db_user","db_env_user"]`), &res); err != nil {
		panic(err)
	}
	for _, v := range res {
		dBUserInfoTypeDbUserTypePropEnum = append(dBUserInfoTypeDbUserTypePropEnum, v)
	}
}

const (

	// DBUserInfoDbUserTypeDbUser captures enum value "db_user"
	DBUserInfoDbUserTypeDbUser string = "db_user"

	// DBUserInfoDbUserTypeDbEnvUser captures enum value "db_env_user"
	DBUserInfoDbUserTypeDbEnvUser string = "db_env_user"
)

// prop value enum
func (m *DBUserInfo) validateDbUserTypeEnum(path, location string, value string) error {
	if err := validate.EnumCase(path, location, value, dBUserInfoTypeDbUserTypePropEnum, true); err != nil {
		return err
	}
	return nil
}

func (m *DBUserInfo) validateDbUserType(formats strfmt.Registry) error {

	if err := validate.Required("dbUserType", "body", m.DbUserType); err != nil {
		return err
	}

	// value enum
	if err := m.validateDbUserTypeEnum("dbUserType", "body", *m.DbUserType); err != nil {
		return err
	}

	return nil
}

func (m *DBUserInfo) validateRoles(formats strfmt.Registry) error {

	if err := validate.Required("roles", "body", m.Roles); err != nil {
		return err
	}

	return nil
}

func (m *DBUserInfo) validateUserID(formats strfmt.Registry) error {

	if err := validate.Required("userId", "body", m.UserID); err != nil {
		return err
	}

	return nil
}

// ContextValidate validates this d b user info based on context it is used
func (m *DBUserInfo) ContextValidate(ctx context.Context, formats strfmt.Registry) error {
	return nil
}

// MarshalBinary interface implementation
func (m *DBUserInfo) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *DBUserInfo) UnmarshalBinary(b []byte) error {
	var res DBUserInfo
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
