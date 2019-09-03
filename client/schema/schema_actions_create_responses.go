//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

// Code generated by go-swagger; DO NOT EDIT.

package schema

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"fmt"
	"io"

	"github.com/go-openapi/runtime"

	strfmt "github.com/go-openapi/strfmt"

	models "github.com/semi-technologies/weaviate/entities/models"
)

// SchemaActionsCreateReader is a Reader for the SchemaActionsCreate structure.
type SchemaActionsCreateReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *SchemaActionsCreateReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {

	case 200:
		result := NewSchemaActionsCreateOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil

	case 401:
		result := NewSchemaActionsCreateUnauthorized()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result

	case 403:
		result := NewSchemaActionsCreateForbidden()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result

	case 422:
		result := NewSchemaActionsCreateUnprocessableEntity()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result

	case 500:
		result := NewSchemaActionsCreateInternalServerError()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result

	default:
		return nil, runtime.NewAPIError("unknown error", response, response.Code())
	}
}

// NewSchemaActionsCreateOK creates a SchemaActionsCreateOK with default headers values
func NewSchemaActionsCreateOK() *SchemaActionsCreateOK {
	return &SchemaActionsCreateOK{}
}

/*SchemaActionsCreateOK handles this case with default header values.

Added the new Action class to the ontology.
*/
type SchemaActionsCreateOK struct {
	Payload *models.Class
}

func (o *SchemaActionsCreateOK) Error() string {
	return fmt.Sprintf("[POST /schema/actions][%d] schemaActionsCreateOK  %+v", 200, o.Payload)
}

func (o *SchemaActionsCreateOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.Class)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewSchemaActionsCreateUnauthorized creates a SchemaActionsCreateUnauthorized with default headers values
func NewSchemaActionsCreateUnauthorized() *SchemaActionsCreateUnauthorized {
	return &SchemaActionsCreateUnauthorized{}
}

/*SchemaActionsCreateUnauthorized handles this case with default header values.

Unauthorized or invalid credentials.
*/
type SchemaActionsCreateUnauthorized struct {
}

func (o *SchemaActionsCreateUnauthorized) Error() string {
	return fmt.Sprintf("[POST /schema/actions][%d] schemaActionsCreateUnauthorized ", 401)
}

func (o *SchemaActionsCreateUnauthorized) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewSchemaActionsCreateForbidden creates a SchemaActionsCreateForbidden with default headers values
func NewSchemaActionsCreateForbidden() *SchemaActionsCreateForbidden {
	return &SchemaActionsCreateForbidden{}
}

/*SchemaActionsCreateForbidden handles this case with default header values.

Forbidden
*/
type SchemaActionsCreateForbidden struct {
	Payload *models.ErrorResponse
}

func (o *SchemaActionsCreateForbidden) Error() string {
	return fmt.Sprintf("[POST /schema/actions][%d] schemaActionsCreateForbidden  %+v", 403, o.Payload)
}

func (o *SchemaActionsCreateForbidden) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewSchemaActionsCreateUnprocessableEntity creates a SchemaActionsCreateUnprocessableEntity with default headers values
func NewSchemaActionsCreateUnprocessableEntity() *SchemaActionsCreateUnprocessableEntity {
	return &SchemaActionsCreateUnprocessableEntity{}
}

/*SchemaActionsCreateUnprocessableEntity handles this case with default header values.

Invalid Action class
*/
type SchemaActionsCreateUnprocessableEntity struct {
	Payload *models.ErrorResponse
}

func (o *SchemaActionsCreateUnprocessableEntity) Error() string {
	return fmt.Sprintf("[POST /schema/actions][%d] schemaActionsCreateUnprocessableEntity  %+v", 422, o.Payload)
}

func (o *SchemaActionsCreateUnprocessableEntity) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewSchemaActionsCreateInternalServerError creates a SchemaActionsCreateInternalServerError with default headers values
func NewSchemaActionsCreateInternalServerError() *SchemaActionsCreateInternalServerError {
	return &SchemaActionsCreateInternalServerError{}
}

/*SchemaActionsCreateInternalServerError handles this case with default header values.

An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.
*/
type SchemaActionsCreateInternalServerError struct {
	Payload *models.ErrorResponse
}

func (o *SchemaActionsCreateInternalServerError) Error() string {
	return fmt.Sprintf("[POST /schema/actions][%d] schemaActionsCreateInternalServerError  %+v", 500, o.Payload)
}

func (o *SchemaActionsCreateInternalServerError) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}
