// Code generated by go-swagger; DO NOT EDIT.

package classifications

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"fmt"
	"io"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"

	"github.com/weaviate/weaviate/entities/models"
)

// ClassificationsGetReader is a Reader for the ClassificationsGet structure.
type ClassificationsGetReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *ClassificationsGetReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewClassificationsGetOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	case 401:
		result := NewClassificationsGetUnauthorized()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 403:
		result := NewClassificationsGetForbidden()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 404:
		result := NewClassificationsGetNotFound()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 500:
		result := NewClassificationsGetInternalServerError()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	default:
		return nil, runtime.NewAPIError("response status code does not match any response statuses defined for this endpoint in the swagger spec", response, response.Code())
	}
}

// NewClassificationsGetOK creates a ClassificationsGetOK with default headers values
func NewClassificationsGetOK() *ClassificationsGetOK {
	return &ClassificationsGetOK{}
}

/*
ClassificationsGetOK describes a response with status code 200, with default header values.

Found the classification, returned as body
*/
type ClassificationsGetOK struct {
	Payload *models.Classification
}

// IsSuccess returns true when this classifications get o k response has a 2xx status code
func (o *ClassificationsGetOK) IsSuccess() bool {
	return true
}

// IsRedirect returns true when this classifications get o k response has a 3xx status code
func (o *ClassificationsGetOK) IsRedirect() bool {
	return false
}

// IsClientError returns true when this classifications get o k response has a 4xx status code
func (o *ClassificationsGetOK) IsClientError() bool {
	return false
}

// IsServerError returns true when this classifications get o k response has a 5xx status code
func (o *ClassificationsGetOK) IsServerError() bool {
	return false
}

// IsCode returns true when this classifications get o k response a status code equal to that given
func (o *ClassificationsGetOK) IsCode(code int) bool {
	return code == 200
}

// Code gets the status code for the classifications get o k response
func (o *ClassificationsGetOK) Code() int {
	return 200
}

func (o *ClassificationsGetOK) Error() string {
	return fmt.Sprintf("[GET /classifications/{id}][%d] classificationsGetOK  %+v", 200, o.Payload)
}

func (o *ClassificationsGetOK) String() string {
	return fmt.Sprintf("[GET /classifications/{id}][%d] classificationsGetOK  %+v", 200, o.Payload)
}

func (o *ClassificationsGetOK) GetPayload() *models.Classification {
	return o.Payload
}

func (o *ClassificationsGetOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.Classification)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewClassificationsGetUnauthorized creates a ClassificationsGetUnauthorized with default headers values
func NewClassificationsGetUnauthorized() *ClassificationsGetUnauthorized {
	return &ClassificationsGetUnauthorized{}
}

/*
ClassificationsGetUnauthorized describes a response with status code 401, with default header values.

Unauthorized or invalid credentials.
*/
type ClassificationsGetUnauthorized struct {
}

// IsSuccess returns true when this classifications get unauthorized response has a 2xx status code
func (o *ClassificationsGetUnauthorized) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this classifications get unauthorized response has a 3xx status code
func (o *ClassificationsGetUnauthorized) IsRedirect() bool {
	return false
}

// IsClientError returns true when this classifications get unauthorized response has a 4xx status code
func (o *ClassificationsGetUnauthorized) IsClientError() bool {
	return true
}

// IsServerError returns true when this classifications get unauthorized response has a 5xx status code
func (o *ClassificationsGetUnauthorized) IsServerError() bool {
	return false
}

// IsCode returns true when this classifications get unauthorized response a status code equal to that given
func (o *ClassificationsGetUnauthorized) IsCode(code int) bool {
	return code == 401
}

// Code gets the status code for the classifications get unauthorized response
func (o *ClassificationsGetUnauthorized) Code() int {
	return 401
}

func (o *ClassificationsGetUnauthorized) Error() string {
	return fmt.Sprintf("[GET /classifications/{id}][%d] classificationsGetUnauthorized ", 401)
}

func (o *ClassificationsGetUnauthorized) String() string {
	return fmt.Sprintf("[GET /classifications/{id}][%d] classificationsGetUnauthorized ", 401)
}

func (o *ClassificationsGetUnauthorized) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewClassificationsGetForbidden creates a ClassificationsGetForbidden with default headers values
func NewClassificationsGetForbidden() *ClassificationsGetForbidden {
	return &ClassificationsGetForbidden{}
}

/*
ClassificationsGetForbidden describes a response with status code 403, with default header values.

Forbidden
*/
type ClassificationsGetForbidden struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this classifications get forbidden response has a 2xx status code
func (o *ClassificationsGetForbidden) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this classifications get forbidden response has a 3xx status code
func (o *ClassificationsGetForbidden) IsRedirect() bool {
	return false
}

// IsClientError returns true when this classifications get forbidden response has a 4xx status code
func (o *ClassificationsGetForbidden) IsClientError() bool {
	return true
}

// IsServerError returns true when this classifications get forbidden response has a 5xx status code
func (o *ClassificationsGetForbidden) IsServerError() bool {
	return false
}

// IsCode returns true when this classifications get forbidden response a status code equal to that given
func (o *ClassificationsGetForbidden) IsCode(code int) bool {
	return code == 403
}

// Code gets the status code for the classifications get forbidden response
func (o *ClassificationsGetForbidden) Code() int {
	return 403
}

func (o *ClassificationsGetForbidden) Error() string {
	return fmt.Sprintf("[GET /classifications/{id}][%d] classificationsGetForbidden  %+v", 403, o.Payload)
}

func (o *ClassificationsGetForbidden) String() string {
	return fmt.Sprintf("[GET /classifications/{id}][%d] classificationsGetForbidden  %+v", 403, o.Payload)
}

func (o *ClassificationsGetForbidden) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ClassificationsGetForbidden) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewClassificationsGetNotFound creates a ClassificationsGetNotFound with default headers values
func NewClassificationsGetNotFound() *ClassificationsGetNotFound {
	return &ClassificationsGetNotFound{}
}

/*
ClassificationsGetNotFound describes a response with status code 404, with default header values.

Not Found - Classification does not exist
*/
type ClassificationsGetNotFound struct {
}

// IsSuccess returns true when this classifications get not found response has a 2xx status code
func (o *ClassificationsGetNotFound) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this classifications get not found response has a 3xx status code
func (o *ClassificationsGetNotFound) IsRedirect() bool {
	return false
}

// IsClientError returns true when this classifications get not found response has a 4xx status code
func (o *ClassificationsGetNotFound) IsClientError() bool {
	return true
}

// IsServerError returns true when this classifications get not found response has a 5xx status code
func (o *ClassificationsGetNotFound) IsServerError() bool {
	return false
}

// IsCode returns true when this classifications get not found response a status code equal to that given
func (o *ClassificationsGetNotFound) IsCode(code int) bool {
	return code == 404
}

// Code gets the status code for the classifications get not found response
func (o *ClassificationsGetNotFound) Code() int {
	return 404
}

func (o *ClassificationsGetNotFound) Error() string {
	return fmt.Sprintf("[GET /classifications/{id}][%d] classificationsGetNotFound ", 404)
}

func (o *ClassificationsGetNotFound) String() string {
	return fmt.Sprintf("[GET /classifications/{id}][%d] classificationsGetNotFound ", 404)
}

func (o *ClassificationsGetNotFound) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewClassificationsGetInternalServerError creates a ClassificationsGetInternalServerError with default headers values
func NewClassificationsGetInternalServerError() *ClassificationsGetInternalServerError {
	return &ClassificationsGetInternalServerError{}
}

/*
ClassificationsGetInternalServerError describes a response with status code 500, with default header values.

An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.
*/
type ClassificationsGetInternalServerError struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this classifications get internal server error response has a 2xx status code
func (o *ClassificationsGetInternalServerError) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this classifications get internal server error response has a 3xx status code
func (o *ClassificationsGetInternalServerError) IsRedirect() bool {
	return false
}

// IsClientError returns true when this classifications get internal server error response has a 4xx status code
func (o *ClassificationsGetInternalServerError) IsClientError() bool {
	return false
}

// IsServerError returns true when this classifications get internal server error response has a 5xx status code
func (o *ClassificationsGetInternalServerError) IsServerError() bool {
	return true
}

// IsCode returns true when this classifications get internal server error response a status code equal to that given
func (o *ClassificationsGetInternalServerError) IsCode(code int) bool {
	return code == 500
}

// Code gets the status code for the classifications get internal server error response
func (o *ClassificationsGetInternalServerError) Code() int {
	return 500
}

func (o *ClassificationsGetInternalServerError) Error() string {
	return fmt.Sprintf("[GET /classifications/{id}][%d] classificationsGetInternalServerError  %+v", 500, o.Payload)
}

func (o *ClassificationsGetInternalServerError) String() string {
	return fmt.Sprintf("[GET /classifications/{id}][%d] classificationsGetInternalServerError  %+v", 500, o.Payload)
}

func (o *ClassificationsGetInternalServerError) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ClassificationsGetInternalServerError) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}
