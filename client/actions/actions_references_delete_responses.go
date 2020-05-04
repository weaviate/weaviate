//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

// Code generated by go-swagger; DO NOT EDIT.

package actions

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"fmt"
	"io"

	"github.com/go-openapi/runtime"

	strfmt "github.com/go-openapi/strfmt"

	models "github.com/semi-technologies/weaviate/entities/models"
)

// ActionsReferencesDeleteReader is a Reader for the ActionsReferencesDelete structure.
type ActionsReferencesDeleteReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *ActionsReferencesDeleteReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 204:
		result := NewActionsReferencesDeleteNoContent()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	case 401:
		result := NewActionsReferencesDeleteUnauthorized()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 403:
		result := NewActionsReferencesDeleteForbidden()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 404:
		result := NewActionsReferencesDeleteNotFound()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 500:
		result := NewActionsReferencesDeleteInternalServerError()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result

	default:
		return nil, runtime.NewAPIError("unknown error", response, response.Code())
	}
}

// NewActionsReferencesDeleteNoContent creates a ActionsReferencesDeleteNoContent with default headers values
func NewActionsReferencesDeleteNoContent() *ActionsReferencesDeleteNoContent {
	return &ActionsReferencesDeleteNoContent{}
}

/*ActionsReferencesDeleteNoContent handles this case with default header values.

Successfully deleted.
*/
type ActionsReferencesDeleteNoContent struct {
}

func (o *ActionsReferencesDeleteNoContent) Error() string {
	return fmt.Sprintf("[DELETE /actions/{id}/references/{propertyName}][%d] actionsReferencesDeleteNoContent ", 204)
}

func (o *ActionsReferencesDeleteNoContent) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewActionsReferencesDeleteUnauthorized creates a ActionsReferencesDeleteUnauthorized with default headers values
func NewActionsReferencesDeleteUnauthorized() *ActionsReferencesDeleteUnauthorized {
	return &ActionsReferencesDeleteUnauthorized{}
}

/*ActionsReferencesDeleteUnauthorized handles this case with default header values.

Unauthorized or invalid credentials.
*/
type ActionsReferencesDeleteUnauthorized struct {
}

func (o *ActionsReferencesDeleteUnauthorized) Error() string {
	return fmt.Sprintf("[DELETE /actions/{id}/references/{propertyName}][%d] actionsReferencesDeleteUnauthorized ", 401)
}

func (o *ActionsReferencesDeleteUnauthorized) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewActionsReferencesDeleteForbidden creates a ActionsReferencesDeleteForbidden with default headers values
func NewActionsReferencesDeleteForbidden() *ActionsReferencesDeleteForbidden {
	return &ActionsReferencesDeleteForbidden{}
}

/*ActionsReferencesDeleteForbidden handles this case with default header values.

Forbidden
*/
type ActionsReferencesDeleteForbidden struct {
	Payload *models.ErrorResponse
}

func (o *ActionsReferencesDeleteForbidden) Error() string {
	return fmt.Sprintf("[DELETE /actions/{id}/references/{propertyName}][%d] actionsReferencesDeleteForbidden  %+v", 403, o.Payload)
}

func (o *ActionsReferencesDeleteForbidden) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ActionsReferencesDeleteForbidden) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewActionsReferencesDeleteNotFound creates a ActionsReferencesDeleteNotFound with default headers values
func NewActionsReferencesDeleteNotFound() *ActionsReferencesDeleteNotFound {
	return &ActionsReferencesDeleteNotFound{}
}

/*ActionsReferencesDeleteNotFound handles this case with default header values.

Successful query result but no resource was found.
*/
type ActionsReferencesDeleteNotFound struct {
	Payload *models.ErrorResponse
}

func (o *ActionsReferencesDeleteNotFound) Error() string {
	return fmt.Sprintf("[DELETE /actions/{id}/references/{propertyName}][%d] actionsReferencesDeleteNotFound  %+v", 404, o.Payload)
}

func (o *ActionsReferencesDeleteNotFound) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ActionsReferencesDeleteNotFound) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewActionsReferencesDeleteInternalServerError creates a ActionsReferencesDeleteInternalServerError with default headers values
func NewActionsReferencesDeleteInternalServerError() *ActionsReferencesDeleteInternalServerError {
	return &ActionsReferencesDeleteInternalServerError{}
}

/*ActionsReferencesDeleteInternalServerError handles this case with default header values.

An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.
*/
type ActionsReferencesDeleteInternalServerError struct {
	Payload *models.ErrorResponse
}

func (o *ActionsReferencesDeleteInternalServerError) Error() string {
	return fmt.Sprintf("[DELETE /actions/{id}/references/{propertyName}][%d] actionsReferencesDeleteInternalServerError  %+v", 500, o.Payload)
}

func (o *ActionsReferencesDeleteInternalServerError) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ActionsReferencesDeleteInternalServerError) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}
