// Code generated by go-swagger; DO NOT EDIT.

package schema

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"fmt"
	"io"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"

	"github.com/semi-technologies/weaviate/entities/models"
)

// SchemaDumpReader is a Reader for the SchemaDump structure.
type SchemaDumpReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *SchemaDumpReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewSchemaDumpOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	case 401:
		result := NewSchemaDumpUnauthorized()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 403:
		result := NewSchemaDumpForbidden()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 500:
		result := NewSchemaDumpInternalServerError()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result

	default:
		return nil, runtime.NewAPIError("unknown error", response, response.Code())
	}
}

// NewSchemaDumpOK creates a SchemaDumpOK with default headers values
func NewSchemaDumpOK() *SchemaDumpOK {
	return &SchemaDumpOK{}
}

/*SchemaDumpOK handles this case with default header values.

Successfully dumped the database schema.
*/
type SchemaDumpOK struct {
	Payload *models.Schema
}

func (o *SchemaDumpOK) Error() string {
	return fmt.Sprintf("[GET /schema][%d] schemaDumpOK  %+v", 200, o.Payload)
}

func (o *SchemaDumpOK) GetPayload() *models.Schema {
	return o.Payload
}

func (o *SchemaDumpOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.Schema)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewSchemaDumpUnauthorized creates a SchemaDumpUnauthorized with default headers values
func NewSchemaDumpUnauthorized() *SchemaDumpUnauthorized {
	return &SchemaDumpUnauthorized{}
}

/*SchemaDumpUnauthorized handles this case with default header values.

Unauthorized or invalid credentials.
*/
type SchemaDumpUnauthorized struct {
}

func (o *SchemaDumpUnauthorized) Error() string {
	return fmt.Sprintf("[GET /schema][%d] schemaDumpUnauthorized ", 401)
}

func (o *SchemaDumpUnauthorized) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewSchemaDumpForbidden creates a SchemaDumpForbidden with default headers values
func NewSchemaDumpForbidden() *SchemaDumpForbidden {
	return &SchemaDumpForbidden{}
}

/*SchemaDumpForbidden handles this case with default header values.

Forbidden
*/
type SchemaDumpForbidden struct {
	Payload *models.ErrorResponse
}

func (o *SchemaDumpForbidden) Error() string {
	return fmt.Sprintf("[GET /schema][%d] schemaDumpForbidden  %+v", 403, o.Payload)
}

func (o *SchemaDumpForbidden) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *SchemaDumpForbidden) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewSchemaDumpInternalServerError creates a SchemaDumpInternalServerError with default headers values
func NewSchemaDumpInternalServerError() *SchemaDumpInternalServerError {
	return &SchemaDumpInternalServerError{}
}

/*SchemaDumpInternalServerError handles this case with default header values.

An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.
*/
type SchemaDumpInternalServerError struct {
	Payload *models.ErrorResponse
}

func (o *SchemaDumpInternalServerError) Error() string {
	return fmt.Sprintf("[GET /schema][%d] schemaDumpInternalServerError  %+v", 500, o.Payload)
}

func (o *SchemaDumpInternalServerError) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *SchemaDumpInternalServerError) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}
