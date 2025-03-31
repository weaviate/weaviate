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

package replication

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"fmt"
	"io"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"

	"github.com/weaviate/weaviate/entities/models"
)

// ReplicationDetailsReader is a Reader for the ReplicationDetails structure.
type ReplicationDetailsReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *ReplicationDetailsReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewReplicationDetailsOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	case 400:
		result := NewReplicationDetailsBadRequest()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 401:
		result := NewReplicationDetailsUnauthorized()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 403:
		result := NewReplicationDetailsForbidden()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 404:
		result := NewReplicationDetailsNotFound()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 500:
		result := NewReplicationDetailsInternalServerError()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 501:
		result := NewReplicationDetailsNotImplemented()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	default:
		return nil, runtime.NewAPIError("response status code does not match any response statuses defined for this endpoint in the swagger spec", response, response.Code())
	}
}

// NewReplicationDetailsOK creates a ReplicationDetailsOK with default headers values
func NewReplicationDetailsOK() *ReplicationDetailsOK {
	return &ReplicationDetailsOK{}
}

/*
ReplicationDetailsOK describes a response with status code 200, with default header values.

The details of the replication operation.
*/
type ReplicationDetailsOK struct {
	Payload *models.ReplicationReplicateDetailsReplicaResponse
}

// IsSuccess returns true when this replication details o k response has a 2xx status code
func (o *ReplicationDetailsOK) IsSuccess() bool {
	return true
}

// IsRedirect returns true when this replication details o k response has a 3xx status code
func (o *ReplicationDetailsOK) IsRedirect() bool {
	return false
}

// IsClientError returns true when this replication details o k response has a 4xx status code
func (o *ReplicationDetailsOK) IsClientError() bool {
	return false
}

// IsServerError returns true when this replication details o k response has a 5xx status code
func (o *ReplicationDetailsOK) IsServerError() bool {
	return false
}

// IsCode returns true when this replication details o k response a status code equal to that given
func (o *ReplicationDetailsOK) IsCode(code int) bool {
	return code == 200
}

// Code gets the status code for the replication details o k response
func (o *ReplicationDetailsOK) Code() int {
	return 200
}

func (o *ReplicationDetailsOK) Error() string {
	return fmt.Sprintf("[GET /replication/replicate/{id}][%d] replicationDetailsOK  %+v", 200, o.Payload)
}

func (o *ReplicationDetailsOK) String() string {
	return fmt.Sprintf("[GET /replication/replicate/{id}][%d] replicationDetailsOK  %+v", 200, o.Payload)
}

func (o *ReplicationDetailsOK) GetPayload() *models.ReplicationReplicateDetailsReplicaResponse {
	return o.Payload
}

func (o *ReplicationDetailsOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ReplicationReplicateDetailsReplicaResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewReplicationDetailsBadRequest creates a ReplicationDetailsBadRequest with default headers values
func NewReplicationDetailsBadRequest() *ReplicationDetailsBadRequest {
	return &ReplicationDetailsBadRequest{}
}

/*
ReplicationDetailsBadRequest describes a response with status code 400, with default header values.

Malformed request.
*/
type ReplicationDetailsBadRequest struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this replication details bad request response has a 2xx status code
func (o *ReplicationDetailsBadRequest) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this replication details bad request response has a 3xx status code
func (o *ReplicationDetailsBadRequest) IsRedirect() bool {
	return false
}

// IsClientError returns true when this replication details bad request response has a 4xx status code
func (o *ReplicationDetailsBadRequest) IsClientError() bool {
	return true
}

// IsServerError returns true when this replication details bad request response has a 5xx status code
func (o *ReplicationDetailsBadRequest) IsServerError() bool {
	return false
}

// IsCode returns true when this replication details bad request response a status code equal to that given
func (o *ReplicationDetailsBadRequest) IsCode(code int) bool {
	return code == 400
}

// Code gets the status code for the replication details bad request response
func (o *ReplicationDetailsBadRequest) Code() int {
	return 400
}

func (o *ReplicationDetailsBadRequest) Error() string {
	return fmt.Sprintf("[GET /replication/replicate/{id}][%d] replicationDetailsBadRequest  %+v", 400, o.Payload)
}

func (o *ReplicationDetailsBadRequest) String() string {
	return fmt.Sprintf("[GET /replication/replicate/{id}][%d] replicationDetailsBadRequest  %+v", 400, o.Payload)
}

func (o *ReplicationDetailsBadRequest) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ReplicationDetailsBadRequest) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewReplicationDetailsUnauthorized creates a ReplicationDetailsUnauthorized with default headers values
func NewReplicationDetailsUnauthorized() *ReplicationDetailsUnauthorized {
	return &ReplicationDetailsUnauthorized{}
}

/*
ReplicationDetailsUnauthorized describes a response with status code 401, with default header values.

Unauthorized or invalid credentials.
*/
type ReplicationDetailsUnauthorized struct {
}

// IsSuccess returns true when this replication details unauthorized response has a 2xx status code
func (o *ReplicationDetailsUnauthorized) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this replication details unauthorized response has a 3xx status code
func (o *ReplicationDetailsUnauthorized) IsRedirect() bool {
	return false
}

// IsClientError returns true when this replication details unauthorized response has a 4xx status code
func (o *ReplicationDetailsUnauthorized) IsClientError() bool {
	return true
}

// IsServerError returns true when this replication details unauthorized response has a 5xx status code
func (o *ReplicationDetailsUnauthorized) IsServerError() bool {
	return false
}

// IsCode returns true when this replication details unauthorized response a status code equal to that given
func (o *ReplicationDetailsUnauthorized) IsCode(code int) bool {
	return code == 401
}

// Code gets the status code for the replication details unauthorized response
func (o *ReplicationDetailsUnauthorized) Code() int {
	return 401
}

func (o *ReplicationDetailsUnauthorized) Error() string {
	return fmt.Sprintf("[GET /replication/replicate/{id}][%d] replicationDetailsUnauthorized ", 401)
}

func (o *ReplicationDetailsUnauthorized) String() string {
	return fmt.Sprintf("[GET /replication/replicate/{id}][%d] replicationDetailsUnauthorized ", 401)
}

func (o *ReplicationDetailsUnauthorized) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewReplicationDetailsForbidden creates a ReplicationDetailsForbidden with default headers values
func NewReplicationDetailsForbidden() *ReplicationDetailsForbidden {
	return &ReplicationDetailsForbidden{}
}

/*
ReplicationDetailsForbidden describes a response with status code 403, with default header values.

Forbidden
*/
type ReplicationDetailsForbidden struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this replication details forbidden response has a 2xx status code
func (o *ReplicationDetailsForbidden) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this replication details forbidden response has a 3xx status code
func (o *ReplicationDetailsForbidden) IsRedirect() bool {
	return false
}

// IsClientError returns true when this replication details forbidden response has a 4xx status code
func (o *ReplicationDetailsForbidden) IsClientError() bool {
	return true
}

// IsServerError returns true when this replication details forbidden response has a 5xx status code
func (o *ReplicationDetailsForbidden) IsServerError() bool {
	return false
}

// IsCode returns true when this replication details forbidden response a status code equal to that given
func (o *ReplicationDetailsForbidden) IsCode(code int) bool {
	return code == 403
}

// Code gets the status code for the replication details forbidden response
func (o *ReplicationDetailsForbidden) Code() int {
	return 403
}

func (o *ReplicationDetailsForbidden) Error() string {
	return fmt.Sprintf("[GET /replication/replicate/{id}][%d] replicationDetailsForbidden  %+v", 403, o.Payload)
}

func (o *ReplicationDetailsForbidden) String() string {
	return fmt.Sprintf("[GET /replication/replicate/{id}][%d] replicationDetailsForbidden  %+v", 403, o.Payload)
}

func (o *ReplicationDetailsForbidden) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ReplicationDetailsForbidden) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewReplicationDetailsNotFound creates a ReplicationDetailsNotFound with default headers values
func NewReplicationDetailsNotFound() *ReplicationDetailsNotFound {
	return &ReplicationDetailsNotFound{}
}

/*
ReplicationDetailsNotFound describes a response with status code 404, with default header values.

Shard replica operation not found
*/
type ReplicationDetailsNotFound struct {
}

// IsSuccess returns true when this replication details not found response has a 2xx status code
func (o *ReplicationDetailsNotFound) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this replication details not found response has a 3xx status code
func (o *ReplicationDetailsNotFound) IsRedirect() bool {
	return false
}

// IsClientError returns true when this replication details not found response has a 4xx status code
func (o *ReplicationDetailsNotFound) IsClientError() bool {
	return true
}

// IsServerError returns true when this replication details not found response has a 5xx status code
func (o *ReplicationDetailsNotFound) IsServerError() bool {
	return false
}

// IsCode returns true when this replication details not found response a status code equal to that given
func (o *ReplicationDetailsNotFound) IsCode(code int) bool {
	return code == 404
}

// Code gets the status code for the replication details not found response
func (o *ReplicationDetailsNotFound) Code() int {
	return 404
}

func (o *ReplicationDetailsNotFound) Error() string {
	return fmt.Sprintf("[GET /replication/replicate/{id}][%d] replicationDetailsNotFound ", 404)
}

func (o *ReplicationDetailsNotFound) String() string {
	return fmt.Sprintf("[GET /replication/replicate/{id}][%d] replicationDetailsNotFound ", 404)
}

func (o *ReplicationDetailsNotFound) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewReplicationDetailsInternalServerError creates a ReplicationDetailsInternalServerError with default headers values
func NewReplicationDetailsInternalServerError() *ReplicationDetailsInternalServerError {
	return &ReplicationDetailsInternalServerError{}
}

/*
ReplicationDetailsInternalServerError describes a response with status code 500, with default header values.

An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.
*/
type ReplicationDetailsInternalServerError struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this replication details internal server error response has a 2xx status code
func (o *ReplicationDetailsInternalServerError) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this replication details internal server error response has a 3xx status code
func (o *ReplicationDetailsInternalServerError) IsRedirect() bool {
	return false
}

// IsClientError returns true when this replication details internal server error response has a 4xx status code
func (o *ReplicationDetailsInternalServerError) IsClientError() bool {
	return false
}

// IsServerError returns true when this replication details internal server error response has a 5xx status code
func (o *ReplicationDetailsInternalServerError) IsServerError() bool {
	return true
}

// IsCode returns true when this replication details internal server error response a status code equal to that given
func (o *ReplicationDetailsInternalServerError) IsCode(code int) bool {
	return code == 500
}

// Code gets the status code for the replication details internal server error response
func (o *ReplicationDetailsInternalServerError) Code() int {
	return 500
}

func (o *ReplicationDetailsInternalServerError) Error() string {
	return fmt.Sprintf("[GET /replication/replicate/{id}][%d] replicationDetailsInternalServerError  %+v", 500, o.Payload)
}

func (o *ReplicationDetailsInternalServerError) String() string {
	return fmt.Sprintf("[GET /replication/replicate/{id}][%d] replicationDetailsInternalServerError  %+v", 500, o.Payload)
}

func (o *ReplicationDetailsInternalServerError) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ReplicationDetailsInternalServerError) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewReplicationDetailsNotImplemented creates a ReplicationDetailsNotImplemented with default headers values
func NewReplicationDetailsNotImplemented() *ReplicationDetailsNotImplemented {
	return &ReplicationDetailsNotImplemented{}
}

/*
ReplicationDetailsNotImplemented describes a response with status code 501, with default header values.

An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.
*/
type ReplicationDetailsNotImplemented struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this replication details not implemented response has a 2xx status code
func (o *ReplicationDetailsNotImplemented) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this replication details not implemented response has a 3xx status code
func (o *ReplicationDetailsNotImplemented) IsRedirect() bool {
	return false
}

// IsClientError returns true when this replication details not implemented response has a 4xx status code
func (o *ReplicationDetailsNotImplemented) IsClientError() bool {
	return false
}

// IsServerError returns true when this replication details not implemented response has a 5xx status code
func (o *ReplicationDetailsNotImplemented) IsServerError() bool {
	return true
}

// IsCode returns true when this replication details not implemented response a status code equal to that given
func (o *ReplicationDetailsNotImplemented) IsCode(code int) bool {
	return code == 501
}

// Code gets the status code for the replication details not implemented response
func (o *ReplicationDetailsNotImplemented) Code() int {
	return 501
}

func (o *ReplicationDetailsNotImplemented) Error() string {
	return fmt.Sprintf("[GET /replication/replicate/{id}][%d] replicationDetailsNotImplemented  %+v", 501, o.Payload)
}

func (o *ReplicationDetailsNotImplemented) String() string {
	return fmt.Sprintf("[GET /replication/replicate/{id}][%d] replicationDetailsNotImplemented  %+v", 501, o.Payload)
}

func (o *ReplicationDetailsNotImplemented) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ReplicationDetailsNotImplemented) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}
