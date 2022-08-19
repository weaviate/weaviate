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

// SchemaObjectsSnapshotsCreateReader is a Reader for the SchemaObjectsSnapshotsCreate structure.
type SchemaObjectsSnapshotsCreateReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *SchemaObjectsSnapshotsCreateReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewSchemaObjectsSnapshotsCreateOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	case 401:
		result := NewSchemaObjectsSnapshotsCreateUnauthorized()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 403:
		result := NewSchemaObjectsSnapshotsCreateForbidden()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 422:
		result := NewSchemaObjectsSnapshotsCreateUnprocessableEntity()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 500:
		result := NewSchemaObjectsSnapshotsCreateInternalServerError()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result

	default:
		return nil, runtime.NewAPIError("unknown error", response, response.Code())
	}
}

// NewSchemaObjectsSnapshotsCreateOK creates a SchemaObjectsSnapshotsCreateOK with default headers values
func NewSchemaObjectsSnapshotsCreateOK() *SchemaObjectsSnapshotsCreateOK {
	return &SchemaObjectsSnapshotsCreateOK{}
}

/*SchemaObjectsSnapshotsCreateOK handles this case with default header values.

Snapshot process successfully started.
*/
type SchemaObjectsSnapshotsCreateOK struct {
	Payload *models.SnapshotMeta
}

func (o *SchemaObjectsSnapshotsCreateOK) Error() string {
	return fmt.Sprintf("[POST /schema/{className}/snapshots/{storageName}/{id}][%d] schemaObjectsSnapshotsCreateOK  %+v", 200, o.Payload)
}

func (o *SchemaObjectsSnapshotsCreateOK) GetPayload() *models.SnapshotMeta {
	return o.Payload
}

func (o *SchemaObjectsSnapshotsCreateOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.SnapshotMeta)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewSchemaObjectsSnapshotsCreateUnauthorized creates a SchemaObjectsSnapshotsCreateUnauthorized with default headers values
func NewSchemaObjectsSnapshotsCreateUnauthorized() *SchemaObjectsSnapshotsCreateUnauthorized {
	return &SchemaObjectsSnapshotsCreateUnauthorized{}
}

/*SchemaObjectsSnapshotsCreateUnauthorized handles this case with default header values.

Unauthorized or invalid credentials.
*/
type SchemaObjectsSnapshotsCreateUnauthorized struct {
}

func (o *SchemaObjectsSnapshotsCreateUnauthorized) Error() string {
	return fmt.Sprintf("[POST /schema/{className}/snapshots/{storageName}/{id}][%d] schemaObjectsSnapshotsCreateUnauthorized ", 401)
}

func (o *SchemaObjectsSnapshotsCreateUnauthorized) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewSchemaObjectsSnapshotsCreateForbidden creates a SchemaObjectsSnapshotsCreateForbidden with default headers values
func NewSchemaObjectsSnapshotsCreateForbidden() *SchemaObjectsSnapshotsCreateForbidden {
	return &SchemaObjectsSnapshotsCreateForbidden{}
}

/*SchemaObjectsSnapshotsCreateForbidden handles this case with default header values.

Forbidden
*/
type SchemaObjectsSnapshotsCreateForbidden struct {
	Payload *models.ErrorResponse
}

func (o *SchemaObjectsSnapshotsCreateForbidden) Error() string {
	return fmt.Sprintf("[POST /schema/{className}/snapshots/{storageName}/{id}][%d] schemaObjectsSnapshotsCreateForbidden  %+v", 403, o.Payload)
}

func (o *SchemaObjectsSnapshotsCreateForbidden) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *SchemaObjectsSnapshotsCreateForbidden) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewSchemaObjectsSnapshotsCreateUnprocessableEntity creates a SchemaObjectsSnapshotsCreateUnprocessableEntity with default headers values
func NewSchemaObjectsSnapshotsCreateUnprocessableEntity() *SchemaObjectsSnapshotsCreateUnprocessableEntity {
	return &SchemaObjectsSnapshotsCreateUnprocessableEntity{}
}

/*SchemaObjectsSnapshotsCreateUnprocessableEntity handles this case with default header values.

Invalid create snapshot attempt.
*/
type SchemaObjectsSnapshotsCreateUnprocessableEntity struct {
	Payload *models.ErrorResponse
}

func (o *SchemaObjectsSnapshotsCreateUnprocessableEntity) Error() string {
	return fmt.Sprintf("[POST /schema/{className}/snapshots/{storageName}/{id}][%d] schemaObjectsSnapshotsCreateUnprocessableEntity  %+v", 422, o.Payload)
}

func (o *SchemaObjectsSnapshotsCreateUnprocessableEntity) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *SchemaObjectsSnapshotsCreateUnprocessableEntity) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewSchemaObjectsSnapshotsCreateInternalServerError creates a SchemaObjectsSnapshotsCreateInternalServerError with default headers values
func NewSchemaObjectsSnapshotsCreateInternalServerError() *SchemaObjectsSnapshotsCreateInternalServerError {
	return &SchemaObjectsSnapshotsCreateInternalServerError{}
}

/*SchemaObjectsSnapshotsCreateInternalServerError handles this case with default header values.

An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.
*/
type SchemaObjectsSnapshotsCreateInternalServerError struct {
	Payload *models.ErrorResponse
}

func (o *SchemaObjectsSnapshotsCreateInternalServerError) Error() string {
	return fmt.Sprintf("[POST /schema/{className}/snapshots/{storageName}/{id}][%d] schemaObjectsSnapshotsCreateInternalServerError  %+v", 500, o.Payload)
}

func (o *SchemaObjectsSnapshotsCreateInternalServerError) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *SchemaObjectsSnapshotsCreateInternalServerError) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}
