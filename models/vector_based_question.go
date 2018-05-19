/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */

package models

import (
	"strconv"

	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/swag"
	"github.com/go-openapi/validate"
)

// VectorBasedQuestion receive question based on array of classes, properties and values.
// swagger:model VectorBasedQuestion

type VectorBasedQuestion []*VectorBasedQuestionItems0

// Validate validates this vector based question
func (m VectorBasedQuestion) Validate(formats strfmt.Registry) error {
	var res []error

	for i := 0; i < len(m); i++ {

		if swag.IsZero(m[i]) { // not required
			continue
		}

		if m[i] != nil {

			if err := m[i].Validate(formats); err != nil {
				if ve, ok := err.(*errors.Validation); ok {
					return ve.ValidateName(strconv.Itoa(i))
				}
				return err
			}
		}

	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

// VectorBasedQuestionItems0 vector based question items0
// swagger:model VectorBasedQuestionItems0

type VectorBasedQuestionItems0 struct {

	// Vectorized properties.
	// Max Items: 300
	// Min Items: 300
	ClassProps []*VectorBasedQuestionItems0ClassPropsItems0 `json:"classProps"`

	// Vectorized classname.
	// Max Items: 300
	// Min Items: 300
	ClassVectors []float32 `json:"classVectors"`
}

/* polymorph VectorBasedQuestionItems0 classProps false */

/* polymorph VectorBasedQuestionItems0 classVectors false */

// Validate validates this vector based question items0
func (m *VectorBasedQuestionItems0) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateClassProps(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if err := m.validateClassVectors(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *VectorBasedQuestionItems0) validateClassProps(formats strfmt.Registry) error {

	if swag.IsZero(m.ClassProps) { // not required
		return nil
	}

	iClassPropsSize := int64(len(m.ClassProps))

	if err := validate.MinItems("classProps", "body", iClassPropsSize, 300); err != nil {
		return err
	}

	if err := validate.MaxItems("classProps", "body", iClassPropsSize, 300); err != nil {
		return err
	}

	for i := 0; i < len(m.ClassProps); i++ {

		if swag.IsZero(m.ClassProps[i]) { // not required
			continue
		}

		if m.ClassProps[i] != nil {

			if err := m.ClassProps[i].Validate(formats); err != nil {
				if ve, ok := err.(*errors.Validation); ok {
					return ve.ValidateName("classProps" + "." + strconv.Itoa(i))
				}
				return err
			}
		}

	}

	return nil
}

func (m *VectorBasedQuestionItems0) validateClassVectors(formats strfmt.Registry) error {

	if swag.IsZero(m.ClassVectors) { // not required
		return nil
	}

	iClassVectorsSize := int64(len(m.ClassVectors))

	if err := validate.MinItems("classVectors", "body", iClassVectorsSize, 300); err != nil {
		return err
	}

	if err := validate.MaxItems("classVectors", "body", iClassVectorsSize, 300); err != nil {
		return err
	}

	return nil
}

// MarshalBinary interface implementation
func (m *VectorBasedQuestionItems0) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *VectorBasedQuestionItems0) UnmarshalBinary(b []byte) error {
	var res VectorBasedQuestionItems0
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}

// VectorBasedQuestionItems0ClassPropsItems0 vector based question items0 class props items0
// swagger:model VectorBasedQuestionItems0ClassPropsItems0

type VectorBasedQuestionItems0ClassPropsItems0 struct {

	// props vectors
	PropsVectors []float32 `json:"propsVectors"`

	// String with valuename.
	Value string `json:"value,omitempty"`
}

/* polymorph VectorBasedQuestionItems0ClassPropsItems0 propsVectors false */

/* polymorph VectorBasedQuestionItems0ClassPropsItems0 value false */

// Validate validates this vector based question items0 class props items0
func (m *VectorBasedQuestionItems0ClassPropsItems0) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validatePropsVectors(formats); err != nil {
		// prop
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *VectorBasedQuestionItems0ClassPropsItems0) validatePropsVectors(formats strfmt.Registry) error {

	if swag.IsZero(m.PropsVectors) { // not required
		return nil
	}

	return nil
}

// MarshalBinary interface implementation
func (m *VectorBasedQuestionItems0ClassPropsItems0) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *VectorBasedQuestionItems0ClassPropsItems0) UnmarshalBinary(b []byte) error {
	var res VectorBasedQuestionItems0ClassPropsItems0
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
