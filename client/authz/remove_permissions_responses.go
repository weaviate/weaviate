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

package authz

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"context"
	"fmt"
	"io"
	"strconv"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
	"github.com/go-openapi/validate"

	"github.com/weaviate/weaviate/entities/models"
)

// RemovePermissionsReader is a Reader for the RemovePermissions structure.
type RemovePermissionsReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *RemovePermissionsReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewRemovePermissionsOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	case 401:
		result := NewRemovePermissionsUnauthorized()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 403:
		result := NewRemovePermissionsForbidden()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 422:
		result := NewRemovePermissionsUnprocessableEntity()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 500:
		result := NewRemovePermissionsInternalServerError()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	default:
		return nil, runtime.NewAPIError("response status code does not match any response statuses defined for this endpoint in the swagger spec", response, response.Code())
	}
}

// NewRemovePermissionsOK creates a RemovePermissionsOK with default headers values
func NewRemovePermissionsOK() *RemovePermissionsOK {
	return &RemovePermissionsOK{}
}

/*
RemovePermissionsOK describes a response with status code 200, with default header values.

Permissions removed successfully
*/
type RemovePermissionsOK struct {
}

// IsSuccess returns true when this remove permissions o k response has a 2xx status code
func (o *RemovePermissionsOK) IsSuccess() bool {
	return true
}

// IsRedirect returns true when this remove permissions o k response has a 3xx status code
func (o *RemovePermissionsOK) IsRedirect() bool {
	return false
}

// IsClientError returns true when this remove permissions o k response has a 4xx status code
func (o *RemovePermissionsOK) IsClientError() bool {
	return false
}

// IsServerError returns true when this remove permissions o k response has a 5xx status code
func (o *RemovePermissionsOK) IsServerError() bool {
	return false
}

// IsCode returns true when this remove permissions o k response a status code equal to that given
func (o *RemovePermissionsOK) IsCode(code int) bool {
	return code == 200
}

// Code gets the status code for the remove permissions o k response
func (o *RemovePermissionsOK) Code() int {
	return 200
}

func (o *RemovePermissionsOK) Error() string {
	return fmt.Sprintf("[POST /authz/roles/remove-permissions][%d] removePermissionsOK ", 200)
}

func (o *RemovePermissionsOK) String() string {
	return fmt.Sprintf("[POST /authz/roles/remove-permissions][%d] removePermissionsOK ", 200)
}

func (o *RemovePermissionsOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewRemovePermissionsUnauthorized creates a RemovePermissionsUnauthorized with default headers values
func NewRemovePermissionsUnauthorized() *RemovePermissionsUnauthorized {
	return &RemovePermissionsUnauthorized{}
}

/*
RemovePermissionsUnauthorized describes a response with status code 401, with default header values.

Unauthorized or invalid credentials.
*/
type RemovePermissionsUnauthorized struct {
}

// IsSuccess returns true when this remove permissions unauthorized response has a 2xx status code
func (o *RemovePermissionsUnauthorized) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this remove permissions unauthorized response has a 3xx status code
func (o *RemovePermissionsUnauthorized) IsRedirect() bool {
	return false
}

// IsClientError returns true when this remove permissions unauthorized response has a 4xx status code
func (o *RemovePermissionsUnauthorized) IsClientError() bool {
	return true
}

// IsServerError returns true when this remove permissions unauthorized response has a 5xx status code
func (o *RemovePermissionsUnauthorized) IsServerError() bool {
	return false
}

// IsCode returns true when this remove permissions unauthorized response a status code equal to that given
func (o *RemovePermissionsUnauthorized) IsCode(code int) bool {
	return code == 401
}

// Code gets the status code for the remove permissions unauthorized response
func (o *RemovePermissionsUnauthorized) Code() int {
	return 401
}

func (o *RemovePermissionsUnauthorized) Error() string {
	return fmt.Sprintf("[POST /authz/roles/remove-permissions][%d] removePermissionsUnauthorized ", 401)
}

func (o *RemovePermissionsUnauthorized) String() string {
	return fmt.Sprintf("[POST /authz/roles/remove-permissions][%d] removePermissionsUnauthorized ", 401)
}

func (o *RemovePermissionsUnauthorized) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewRemovePermissionsForbidden creates a RemovePermissionsForbidden with default headers values
func NewRemovePermissionsForbidden() *RemovePermissionsForbidden {
	return &RemovePermissionsForbidden{}
}

/*
RemovePermissionsForbidden describes a response with status code 403, with default header values.

Forbidden
*/
type RemovePermissionsForbidden struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this remove permissions forbidden response has a 2xx status code
func (o *RemovePermissionsForbidden) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this remove permissions forbidden response has a 3xx status code
func (o *RemovePermissionsForbidden) IsRedirect() bool {
	return false
}

// IsClientError returns true when this remove permissions forbidden response has a 4xx status code
func (o *RemovePermissionsForbidden) IsClientError() bool {
	return true
}

// IsServerError returns true when this remove permissions forbidden response has a 5xx status code
func (o *RemovePermissionsForbidden) IsServerError() bool {
	return false
}

// IsCode returns true when this remove permissions forbidden response a status code equal to that given
func (o *RemovePermissionsForbidden) IsCode(code int) bool {
	return code == 403
}

// Code gets the status code for the remove permissions forbidden response
func (o *RemovePermissionsForbidden) Code() int {
	return 403
}

func (o *RemovePermissionsForbidden) Error() string {
	return fmt.Sprintf("[POST /authz/roles/remove-permissions][%d] removePermissionsForbidden  %+v", 403, o.Payload)
}

func (o *RemovePermissionsForbidden) String() string {
	return fmt.Sprintf("[POST /authz/roles/remove-permissions][%d] removePermissionsForbidden  %+v", 403, o.Payload)
}

func (o *RemovePermissionsForbidden) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *RemovePermissionsForbidden) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewRemovePermissionsUnprocessableEntity creates a RemovePermissionsUnprocessableEntity with default headers values
func NewRemovePermissionsUnprocessableEntity() *RemovePermissionsUnprocessableEntity {
	return &RemovePermissionsUnprocessableEntity{}
}

/*
RemovePermissionsUnprocessableEntity describes a response with status code 422, with default header values.

Request body is well-formed (i.e., syntactically correct), but semantically erroneous. Are you sure the class is defined in the configuration file?
*/
type RemovePermissionsUnprocessableEntity struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this remove permissions unprocessable entity response has a 2xx status code
func (o *RemovePermissionsUnprocessableEntity) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this remove permissions unprocessable entity response has a 3xx status code
func (o *RemovePermissionsUnprocessableEntity) IsRedirect() bool {
	return false
}

// IsClientError returns true when this remove permissions unprocessable entity response has a 4xx status code
func (o *RemovePermissionsUnprocessableEntity) IsClientError() bool {
	return true
}

// IsServerError returns true when this remove permissions unprocessable entity response has a 5xx status code
func (o *RemovePermissionsUnprocessableEntity) IsServerError() bool {
	return false
}

// IsCode returns true when this remove permissions unprocessable entity response a status code equal to that given
func (o *RemovePermissionsUnprocessableEntity) IsCode(code int) bool {
	return code == 422
}

// Code gets the status code for the remove permissions unprocessable entity response
func (o *RemovePermissionsUnprocessableEntity) Code() int {
	return 422
}

func (o *RemovePermissionsUnprocessableEntity) Error() string {
	return fmt.Sprintf("[POST /authz/roles/remove-permissions][%d] removePermissionsUnprocessableEntity  %+v", 422, o.Payload)
}

func (o *RemovePermissionsUnprocessableEntity) String() string {
	return fmt.Sprintf("[POST /authz/roles/remove-permissions][%d] removePermissionsUnprocessableEntity  %+v", 422, o.Payload)
}

func (o *RemovePermissionsUnprocessableEntity) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *RemovePermissionsUnprocessableEntity) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewRemovePermissionsInternalServerError creates a RemovePermissionsInternalServerError with default headers values
func NewRemovePermissionsInternalServerError() *RemovePermissionsInternalServerError {
	return &RemovePermissionsInternalServerError{}
}

/*
RemovePermissionsInternalServerError describes a response with status code 500, with default header values.

An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.
*/
type RemovePermissionsInternalServerError struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this remove permissions internal server error response has a 2xx status code
func (o *RemovePermissionsInternalServerError) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this remove permissions internal server error response has a 3xx status code
func (o *RemovePermissionsInternalServerError) IsRedirect() bool {
	return false
}

// IsClientError returns true when this remove permissions internal server error response has a 4xx status code
func (o *RemovePermissionsInternalServerError) IsClientError() bool {
	return false
}

// IsServerError returns true when this remove permissions internal server error response has a 5xx status code
func (o *RemovePermissionsInternalServerError) IsServerError() bool {
	return true
}

// IsCode returns true when this remove permissions internal server error response a status code equal to that given
func (o *RemovePermissionsInternalServerError) IsCode(code int) bool {
	return code == 500
}

// Code gets the status code for the remove permissions internal server error response
func (o *RemovePermissionsInternalServerError) Code() int {
	return 500
}

func (o *RemovePermissionsInternalServerError) Error() string {
	return fmt.Sprintf("[POST /authz/roles/remove-permissions][%d] removePermissionsInternalServerError  %+v", 500, o.Payload)
}

func (o *RemovePermissionsInternalServerError) String() string {
	return fmt.Sprintf("[POST /authz/roles/remove-permissions][%d] removePermissionsInternalServerError  %+v", 500, o.Payload)
}

func (o *RemovePermissionsInternalServerError) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *RemovePermissionsInternalServerError) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

/*
RemovePermissionsBody remove permissions body
swagger:model RemovePermissionsBody
*/
type RemovePermissionsBody struct {

	// role name
	// Required: true
	Name *string `json:"name"`

	// permissions to remove from the role
	// Required: true
	Permissions []*models.Permission `json:"permissions"`
}

// Validate validates this remove permissions body
func (o *RemovePermissionsBody) Validate(formats strfmt.Registry) error {
	var res []error

	if err := o.validateName(formats); err != nil {
		res = append(res, err)
	}

	if err := o.validatePermissions(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (o *RemovePermissionsBody) validateName(formats strfmt.Registry) error {

	if err := validate.Required("body"+"."+"name", "body", o.Name); err != nil {
		return err
	}

	return nil
}

func (o *RemovePermissionsBody) validatePermissions(formats strfmt.Registry) error {

	if err := validate.Required("body"+"."+"permissions", "body", o.Permissions); err != nil {
		return err
	}

	for i := 0; i < len(o.Permissions); i++ {
		if swag.IsZero(o.Permissions[i]) { // not required
			continue
		}

		if o.Permissions[i] != nil {
			if err := o.Permissions[i].Validate(formats); err != nil {
				if ve, ok := err.(*errors.Validation); ok {
					return ve.ValidateName("body" + "." + "permissions" + "." + strconv.Itoa(i))
				} else if ce, ok := err.(*errors.CompositeError); ok {
					return ce.ValidateName("body" + "." + "permissions" + "." + strconv.Itoa(i))
				}
				return err
			}
		}

	}

	return nil
}

// ContextValidate validate this remove permissions body based on the context it is used
func (o *RemovePermissionsBody) ContextValidate(ctx context.Context, formats strfmt.Registry) error {
	var res []error

	if err := o.contextValidatePermissions(ctx, formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (o *RemovePermissionsBody) contextValidatePermissions(ctx context.Context, formats strfmt.Registry) error {

	for i := 0; i < len(o.Permissions); i++ {

		if o.Permissions[i] != nil {
			if err := o.Permissions[i].ContextValidate(ctx, formats); err != nil {
				if ve, ok := err.(*errors.Validation); ok {
					return ve.ValidateName("body" + "." + "permissions" + "." + strconv.Itoa(i))
				} else if ce, ok := err.(*errors.CompositeError); ok {
					return ce.ValidateName("body" + "." + "permissions" + "." + strconv.Itoa(i))
				}
				return err
			}
		}

	}

	return nil
}

// MarshalBinary interface implementation
func (o *RemovePermissionsBody) MarshalBinary() ([]byte, error) {
	if o == nil {
		return nil, nil
	}
	return swag.WriteJSON(o)
}

// UnmarshalBinary interface implementation
func (o *RemovePermissionsBody) UnmarshalBinary(b []byte) error {
	var res RemovePermissionsBody
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*o = res
	return nil
}
