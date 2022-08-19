// Code generated by go-swagger; DO NOT EDIT.

package objects

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"fmt"
	"io"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"

	"github.com/semi-technologies/weaviate/entities/models"
)

// ObjectsClassPutReader is a Reader for the ObjectsClassPut structure.
type ObjectsClassPutReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *ObjectsClassPutReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewObjectsClassPutOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	case 401:
		result := NewObjectsClassPutUnauthorized()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 403:
		result := NewObjectsClassPutForbidden()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 404:
		result := NewObjectsClassPutNotFound()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 422:
		result := NewObjectsClassPutUnprocessableEntity()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 500:
		result := NewObjectsClassPutInternalServerError()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result

	default:
		return nil, runtime.NewAPIError("unknown error", response, response.Code())
	}
}

// NewObjectsClassPutOK creates a ObjectsClassPutOK with default headers values
func NewObjectsClassPutOK() *ObjectsClassPutOK {
	return &ObjectsClassPutOK{}
}

/*ObjectsClassPutOK handles this case with default header values.

Successfully received.
*/
type ObjectsClassPutOK struct {
	Payload *models.Object
}

func (o *ObjectsClassPutOK) Error() string {
	return fmt.Sprintf("[PUT /objects/{className}/{id}][%d] objectsClassPutOK  %+v", 200, o.Payload)
}

func (o *ObjectsClassPutOK) GetPayload() *models.Object {
	return o.Payload
}

func (o *ObjectsClassPutOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.Object)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewObjectsClassPutUnauthorized creates a ObjectsClassPutUnauthorized with default headers values
func NewObjectsClassPutUnauthorized() *ObjectsClassPutUnauthorized {
	return &ObjectsClassPutUnauthorized{}
}

/*ObjectsClassPutUnauthorized handles this case with default header values.

Unauthorized or invalid credentials.
*/
type ObjectsClassPutUnauthorized struct {
}

func (o *ObjectsClassPutUnauthorized) Error() string {
	return fmt.Sprintf("[PUT /objects/{className}/{id}][%d] objectsClassPutUnauthorized ", 401)
}

func (o *ObjectsClassPutUnauthorized) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewObjectsClassPutForbidden creates a ObjectsClassPutForbidden with default headers values
func NewObjectsClassPutForbidden() *ObjectsClassPutForbidden {
	return &ObjectsClassPutForbidden{}
}

/*ObjectsClassPutForbidden handles this case with default header values.

Forbidden
*/
type ObjectsClassPutForbidden struct {
	Payload *models.ErrorResponse
}

func (o *ObjectsClassPutForbidden) Error() string {
	return fmt.Sprintf("[PUT /objects/{className}/{id}][%d] objectsClassPutForbidden  %+v", 403, o.Payload)
}

func (o *ObjectsClassPutForbidden) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ObjectsClassPutForbidden) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewObjectsClassPutNotFound creates a ObjectsClassPutNotFound with default headers values
func NewObjectsClassPutNotFound() *ObjectsClassPutNotFound {
	return &ObjectsClassPutNotFound{}
}

/*ObjectsClassPutNotFound handles this case with default header values.

Successful query result but no resource was found.
*/
type ObjectsClassPutNotFound struct {
}

func (o *ObjectsClassPutNotFound) Error() string {
	return fmt.Sprintf("[PUT /objects/{className}/{id}][%d] objectsClassPutNotFound ", 404)
}

func (o *ObjectsClassPutNotFound) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewObjectsClassPutUnprocessableEntity creates a ObjectsClassPutUnprocessableEntity with default headers values
func NewObjectsClassPutUnprocessableEntity() *ObjectsClassPutUnprocessableEntity {
	return &ObjectsClassPutUnprocessableEntity{}
}

/*ObjectsClassPutUnprocessableEntity handles this case with default header values.

Request body is well-formed (i.e., syntactically correct), but semantically erroneous. Are you sure the class is defined in the configuration file?
*/
type ObjectsClassPutUnprocessableEntity struct {
	Payload *models.ErrorResponse
}

func (o *ObjectsClassPutUnprocessableEntity) Error() string {
	return fmt.Sprintf("[PUT /objects/{className}/{id}][%d] objectsClassPutUnprocessableEntity  %+v", 422, o.Payload)
}

func (o *ObjectsClassPutUnprocessableEntity) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ObjectsClassPutUnprocessableEntity) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewObjectsClassPutInternalServerError creates a ObjectsClassPutInternalServerError with default headers values
func NewObjectsClassPutInternalServerError() *ObjectsClassPutInternalServerError {
	return &ObjectsClassPutInternalServerError{}
}

/*ObjectsClassPutInternalServerError handles this case with default header values.

An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.
*/
type ObjectsClassPutInternalServerError struct {
	Payload *models.ErrorResponse
}

func (o *ObjectsClassPutInternalServerError) Error() string {
	return fmt.Sprintf("[PUT /objects/{className}/{id}][%d] objectsClassPutInternalServerError  %+v", 500, o.Payload)
}

func (o *ObjectsClassPutInternalServerError) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ObjectsClassPutInternalServerError) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}
