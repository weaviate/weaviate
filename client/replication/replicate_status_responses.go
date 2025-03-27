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
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
	"github.com/go-openapi/validate"

	"github.com/weaviate/weaviate/entities/models"
)

// ReplicateStatusReader is a Reader for the ReplicateStatus structure.
type ReplicateStatusReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *ReplicateStatusReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewReplicateStatusOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	case 400:
		result := NewReplicateStatusBadRequest()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 404:
		result := NewReplicateStatusNotFound()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 501:
		result := NewReplicateStatusNotImplemented()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	default:
		return nil, runtime.NewAPIError("response status code does not match any response statuses defined for this endpoint in the swagger spec", response, response.Code())
	}
}

// NewReplicateStatusOK creates a ReplicateStatusOK with default headers values
func NewReplicateStatusOK() *ReplicateStatusOK {
	return &ReplicateStatusOK{}
}

/*
ReplicateStatusOK describes a response with status code 200, with default header values.

The status of the shard replica move operation
*/
type ReplicateStatusOK struct {
	Payload *ReplicateStatusOKBody
}

// IsSuccess returns true when this replicate status o k response has a 2xx status code
func (o *ReplicateStatusOK) IsSuccess() bool {
	return true
}

// IsRedirect returns true when this replicate status o k response has a 3xx status code
func (o *ReplicateStatusOK) IsRedirect() bool {
	return false
}

// IsClientError returns true when this replicate status o k response has a 4xx status code
func (o *ReplicateStatusOK) IsClientError() bool {
	return false
}

// IsServerError returns true when this replicate status o k response has a 5xx status code
func (o *ReplicateStatusOK) IsServerError() bool {
	return false
}

// IsCode returns true when this replicate status o k response a status code equal to that given
func (o *ReplicateStatusOK) IsCode(code int) bool {
	return code == 200
}

// Code gets the status code for the replicate status o k response
func (o *ReplicateStatusOK) Code() int {
	return 200
}

func (o *ReplicateStatusOK) Error() string {
	return fmt.Sprintf("[GET /replication/replicate/{id}/status][%d] replicateStatusOK  %+v", 200, o.Payload)
}

func (o *ReplicateStatusOK) String() string {
	return fmt.Sprintf("[GET /replication/replicate/{id}/status][%d] replicateStatusOK  %+v", 200, o.Payload)
}

func (o *ReplicateStatusOK) GetPayload() *ReplicateStatusOKBody {
	return o.Payload
}

func (o *ReplicateStatusOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(ReplicateStatusOKBody)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewReplicateStatusBadRequest creates a ReplicateStatusBadRequest with default headers values
func NewReplicateStatusBadRequest() *ReplicateStatusBadRequest {
	return &ReplicateStatusBadRequest{}
}

/*
ReplicateStatusBadRequest describes a response with status code 400, with default header values.

Malformed replica move operation id
*/
type ReplicateStatusBadRequest struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this replicate status bad request response has a 2xx status code
func (o *ReplicateStatusBadRequest) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this replicate status bad request response has a 3xx status code
func (o *ReplicateStatusBadRequest) IsRedirect() bool {
	return false
}

// IsClientError returns true when this replicate status bad request response has a 4xx status code
func (o *ReplicateStatusBadRequest) IsClientError() bool {
	return true
}

// IsServerError returns true when this replicate status bad request response has a 5xx status code
func (o *ReplicateStatusBadRequest) IsServerError() bool {
	return false
}

// IsCode returns true when this replicate status bad request response a status code equal to that given
func (o *ReplicateStatusBadRequest) IsCode(code int) bool {
	return code == 400
}

// Code gets the status code for the replicate status bad request response
func (o *ReplicateStatusBadRequest) Code() int {
	return 400
}

func (o *ReplicateStatusBadRequest) Error() string {
	return fmt.Sprintf("[GET /replication/replicate/{id}/status][%d] replicateStatusBadRequest  %+v", 400, o.Payload)
}

func (o *ReplicateStatusBadRequest) String() string {
	return fmt.Sprintf("[GET /replication/replicate/{id}/status][%d] replicateStatusBadRequest  %+v", 400, o.Payload)
}

func (o *ReplicateStatusBadRequest) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ReplicateStatusBadRequest) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewReplicateStatusNotFound creates a ReplicateStatusNotFound with default headers values
func NewReplicateStatusNotFound() *ReplicateStatusNotFound {
	return &ReplicateStatusNotFound{}
}

/*
ReplicateStatusNotFound describes a response with status code 404, with default header values.

Shard replica move operation not found
*/
type ReplicateStatusNotFound struct {
}

// IsSuccess returns true when this replicate status not found response has a 2xx status code
func (o *ReplicateStatusNotFound) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this replicate status not found response has a 3xx status code
func (o *ReplicateStatusNotFound) IsRedirect() bool {
	return false
}

// IsClientError returns true when this replicate status not found response has a 4xx status code
func (o *ReplicateStatusNotFound) IsClientError() bool {
	return true
}

// IsServerError returns true when this replicate status not found response has a 5xx status code
func (o *ReplicateStatusNotFound) IsServerError() bool {
	return false
}

// IsCode returns true when this replicate status not found response a status code equal to that given
func (o *ReplicateStatusNotFound) IsCode(code int) bool {
	return code == 404
}

// Code gets the status code for the replicate status not found response
func (o *ReplicateStatusNotFound) Code() int {
	return 404
}

func (o *ReplicateStatusNotFound) Error() string {
	return fmt.Sprintf("[GET /replication/replicate/{id}/status][%d] replicateStatusNotFound ", 404)
}

func (o *ReplicateStatusNotFound) String() string {
	return fmt.Sprintf("[GET /replication/replicate/{id}/status][%d] replicateStatusNotFound ", 404)
}

func (o *ReplicateStatusNotFound) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewReplicateStatusNotImplemented creates a ReplicateStatusNotImplemented with default headers values
func NewReplicateStatusNotImplemented() *ReplicateStatusNotImplemented {
	return &ReplicateStatusNotImplemented{}
}

/*
ReplicateStatusNotImplemented describes a response with status code 501, with default header values.

An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.
*/
type ReplicateStatusNotImplemented struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this replicate status not implemented response has a 2xx status code
func (o *ReplicateStatusNotImplemented) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this replicate status not implemented response has a 3xx status code
func (o *ReplicateStatusNotImplemented) IsRedirect() bool {
	return false
}

// IsClientError returns true when this replicate status not implemented response has a 4xx status code
func (o *ReplicateStatusNotImplemented) IsClientError() bool {
	return false
}

// IsServerError returns true when this replicate status not implemented response has a 5xx status code
func (o *ReplicateStatusNotImplemented) IsServerError() bool {
	return true
}

// IsCode returns true when this replicate status not implemented response a status code equal to that given
func (o *ReplicateStatusNotImplemented) IsCode(code int) bool {
	return code == 501
}

// Code gets the status code for the replicate status not implemented response
func (o *ReplicateStatusNotImplemented) Code() int {
	return 501
}

func (o *ReplicateStatusNotImplemented) Error() string {
	return fmt.Sprintf("[GET /replication/replicate/{id}/status][%d] replicateStatusNotImplemented  %+v", 501, o.Payload)
}

func (o *ReplicateStatusNotImplemented) String() string {
	return fmt.Sprintf("[GET /replication/replicate/{id}/status][%d] replicateStatusNotImplemented  %+v", 501, o.Payload)
}

func (o *ReplicateStatusNotImplemented) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ReplicateStatusNotImplemented) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

/*
ReplicateStatusOKBody replicate status o k body
swagger:model ReplicateStatusOKBody
*/
type ReplicateStatusOKBody struct {

	// The current status of the shard replica move operation
	// Required: true
	// Enum: [READY INDEXING REPLICATION_FINALIZING REPLICATION_HYDRATING REPLICATION_DEHYDRATING]
	Status *string `json:"status"`
}

// Validate validates this replicate status o k body
func (o *ReplicateStatusOKBody) Validate(formats strfmt.Registry) error {
	var res []error

	if err := o.validateStatus(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

var replicateStatusOKBodyTypeStatusPropEnum []interface{}

func init() {
	var res []string
	if err := json.Unmarshal([]byte(`["READY","INDEXING","REPLICATION_FINALIZING","REPLICATION_HYDRATING","REPLICATION_DEHYDRATING"]`), &res); err != nil {
		panic(err)
	}
	for _, v := range res {
		replicateStatusOKBodyTypeStatusPropEnum = append(replicateStatusOKBodyTypeStatusPropEnum, v)
	}
}

const (

	// ReplicateStatusOKBodyStatusREADY captures enum value "READY"
	ReplicateStatusOKBodyStatusREADY string = "READY"

	// ReplicateStatusOKBodyStatusINDEXING captures enum value "INDEXING"
	ReplicateStatusOKBodyStatusINDEXING string = "INDEXING"

	// ReplicateStatusOKBodyStatusREPLICATIONFINALIZING captures enum value "REPLICATION_FINALIZING"
	ReplicateStatusOKBodyStatusREPLICATIONFINALIZING string = "REPLICATION_FINALIZING"

	// ReplicateStatusOKBodyStatusREPLICATIONHYDRATING captures enum value "REPLICATION_HYDRATING"
	ReplicateStatusOKBodyStatusREPLICATIONHYDRATING string = "REPLICATION_HYDRATING"

	// ReplicateStatusOKBodyStatusREPLICATIONDEHYDRATING captures enum value "REPLICATION_DEHYDRATING"
	ReplicateStatusOKBodyStatusREPLICATIONDEHYDRATING string = "REPLICATION_DEHYDRATING"
)

// prop value enum
func (o *ReplicateStatusOKBody) validateStatusEnum(path, location string, value string) error {
	if err := validate.EnumCase(path, location, value, replicateStatusOKBodyTypeStatusPropEnum, true); err != nil {
		return err
	}
	return nil
}

func (o *ReplicateStatusOKBody) validateStatus(formats strfmt.Registry) error {

	if err := validate.Required("replicateStatusOK"+"."+"status", "body", o.Status); err != nil {
		return err
	}

	// value enum
	if err := o.validateStatusEnum("replicateStatusOK"+"."+"status", "body", *o.Status); err != nil {
		return err
	}

	return nil
}

// ContextValidate validates this replicate status o k body based on context it is used
func (o *ReplicateStatusOKBody) ContextValidate(ctx context.Context, formats strfmt.Registry) error {
	return nil
}

// MarshalBinary interface implementation
func (o *ReplicateStatusOKBody) MarshalBinary() ([]byte, error) {
	if o == nil {
		return nil, nil
	}
	return swag.WriteJSON(o)
}

// UnmarshalBinary interface implementation
func (o *ReplicateStatusOKBody) UnmarshalBinary(b []byte) error {
	var res ReplicateStatusOKBody
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*o = res
	return nil
}
