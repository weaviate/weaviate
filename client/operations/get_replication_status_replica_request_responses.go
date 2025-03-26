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

package operations

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

// GetReplicationStatusReplicaRequestReader is a Reader for the GetReplicationStatusReplicaRequest structure.
type GetReplicationStatusReplicaRequestReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *GetReplicationStatusReplicaRequestReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewGetReplicationStatusReplicaRequestOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	case 400:
		result := NewGetReplicationStatusReplicaRequestBadRequest()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 404:
		result := NewGetReplicationStatusReplicaRequestNotFound()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	default:
		return nil, runtime.NewAPIError("response status code does not match any response statuses defined for this endpoint in the swagger spec", response, response.Code())
	}
}

// NewGetReplicationStatusReplicaRequestOK creates a GetReplicationStatusReplicaRequestOK with default headers values
func NewGetReplicationStatusReplicaRequestOK() *GetReplicationStatusReplicaRequestOK {
	return &GetReplicationStatusReplicaRequestOK{}
}

/*
GetReplicationStatusReplicaRequestOK describes a response with status code 200, with default header values.

The status of the shard replica move operation
*/
type GetReplicationStatusReplicaRequestOK struct {
	Payload *GetReplicationStatusReplicaRequestOKBody
}

// IsSuccess returns true when this get replication status replica request o k response has a 2xx status code
func (o *GetReplicationStatusReplicaRequestOK) IsSuccess() bool {
	return true
}

// IsRedirect returns true when this get replication status replica request o k response has a 3xx status code
func (o *GetReplicationStatusReplicaRequestOK) IsRedirect() bool {
	return false
}

// IsClientError returns true when this get replication status replica request o k response has a 4xx status code
func (o *GetReplicationStatusReplicaRequestOK) IsClientError() bool {
	return false
}

// IsServerError returns true when this get replication status replica request o k response has a 5xx status code
func (o *GetReplicationStatusReplicaRequestOK) IsServerError() bool {
	return false
}

// IsCode returns true when this get replication status replica request o k response a status code equal to that given
func (o *GetReplicationStatusReplicaRequestOK) IsCode(code int) bool {
	return code == 200
}

// Code gets the status code for the get replication status replica request o k response
func (o *GetReplicationStatusReplicaRequestOK) Code() int {
	return 200
}

func (o *GetReplicationStatusReplicaRequestOK) Error() string {
	return fmt.Sprintf("[GET /replication/{id}/status][%d] getReplicationStatusReplicaRequestOK  %+v", 200, o.Payload)
}

func (o *GetReplicationStatusReplicaRequestOK) String() string {
	return fmt.Sprintf("[GET /replication/{id}/status][%d] getReplicationStatusReplicaRequestOK  %+v", 200, o.Payload)
}

func (o *GetReplicationStatusReplicaRequestOK) GetPayload() *GetReplicationStatusReplicaRequestOKBody {
	return o.Payload
}

func (o *GetReplicationStatusReplicaRequestOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(GetReplicationStatusReplicaRequestOKBody)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewGetReplicationStatusReplicaRequestBadRequest creates a GetReplicationStatusReplicaRequestBadRequest with default headers values
func NewGetReplicationStatusReplicaRequestBadRequest() *GetReplicationStatusReplicaRequestBadRequest {
	return &GetReplicationStatusReplicaRequestBadRequest{}
}

/*
GetReplicationStatusReplicaRequestBadRequest describes a response with status code 400, with default header values.

Malformed replica move operation id
*/
type GetReplicationStatusReplicaRequestBadRequest struct {
	Payload *models.ErrorResponse
}

// IsSuccess returns true when this get replication status replica request bad request response has a 2xx status code
func (o *GetReplicationStatusReplicaRequestBadRequest) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this get replication status replica request bad request response has a 3xx status code
func (o *GetReplicationStatusReplicaRequestBadRequest) IsRedirect() bool {
	return false
}

// IsClientError returns true when this get replication status replica request bad request response has a 4xx status code
func (o *GetReplicationStatusReplicaRequestBadRequest) IsClientError() bool {
	return true
}

// IsServerError returns true when this get replication status replica request bad request response has a 5xx status code
func (o *GetReplicationStatusReplicaRequestBadRequest) IsServerError() bool {
	return false
}

// IsCode returns true when this get replication status replica request bad request response a status code equal to that given
func (o *GetReplicationStatusReplicaRequestBadRequest) IsCode(code int) bool {
	return code == 400
}

// Code gets the status code for the get replication status replica request bad request response
func (o *GetReplicationStatusReplicaRequestBadRequest) Code() int {
	return 400
}

func (o *GetReplicationStatusReplicaRequestBadRequest) Error() string {
	return fmt.Sprintf("[GET /replication/{id}/status][%d] getReplicationStatusReplicaRequestBadRequest  %+v", 400, o.Payload)
}

func (o *GetReplicationStatusReplicaRequestBadRequest) String() string {
	return fmt.Sprintf("[GET /replication/{id}/status][%d] getReplicationStatusReplicaRequestBadRequest  %+v", 400, o.Payload)
}

func (o *GetReplicationStatusReplicaRequestBadRequest) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *GetReplicationStatusReplicaRequestBadRequest) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewGetReplicationStatusReplicaRequestNotFound creates a GetReplicationStatusReplicaRequestNotFound with default headers values
func NewGetReplicationStatusReplicaRequestNotFound() *GetReplicationStatusReplicaRequestNotFound {
	return &GetReplicationStatusReplicaRequestNotFound{}
}

/*
GetReplicationStatusReplicaRequestNotFound describes a response with status code 404, with default header values.

Shard replica move operation not found
*/
type GetReplicationStatusReplicaRequestNotFound struct {
}

// IsSuccess returns true when this get replication status replica request not found response has a 2xx status code
func (o *GetReplicationStatusReplicaRequestNotFound) IsSuccess() bool {
	return false
}

// IsRedirect returns true when this get replication status replica request not found response has a 3xx status code
func (o *GetReplicationStatusReplicaRequestNotFound) IsRedirect() bool {
	return false
}

// IsClientError returns true when this get replication status replica request not found response has a 4xx status code
func (o *GetReplicationStatusReplicaRequestNotFound) IsClientError() bool {
	return true
}

// IsServerError returns true when this get replication status replica request not found response has a 5xx status code
func (o *GetReplicationStatusReplicaRequestNotFound) IsServerError() bool {
	return false
}

// IsCode returns true when this get replication status replica request not found response a status code equal to that given
func (o *GetReplicationStatusReplicaRequestNotFound) IsCode(code int) bool {
	return code == 404
}

// Code gets the status code for the get replication status replica request not found response
func (o *GetReplicationStatusReplicaRequestNotFound) Code() int {
	return 404
}

func (o *GetReplicationStatusReplicaRequestNotFound) Error() string {
	return fmt.Sprintf("[GET /replication/{id}/status][%d] getReplicationStatusReplicaRequestNotFound ", 404)
}

func (o *GetReplicationStatusReplicaRequestNotFound) String() string {
	return fmt.Sprintf("[GET /replication/{id}/status][%d] getReplicationStatusReplicaRequestNotFound ", 404)
}

func (o *GetReplicationStatusReplicaRequestNotFound) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

/*
GetReplicationStatusReplicaRequestOKBody get replication status replica request o k body
swagger:model GetReplicationStatusReplicaRequestOKBody
*/
type GetReplicationStatusReplicaRequestOKBody struct {

	// The current status of the shard replica move operation
	// Required: true
	// Enum: [READY INDEXING REPLICATION_FINALIZING REPLICATION_HYDRATING REPLICATION_DEHYDRATING]
	Status *string `json:"status"`
}

// Validate validates this get replication status replica request o k body
func (o *GetReplicationStatusReplicaRequestOKBody) Validate(formats strfmt.Registry) error {
	var res []error

	if err := o.validateStatus(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

var getReplicationStatusReplicaRequestOKBodyTypeStatusPropEnum []interface{}

func init() {
	var res []string
	if err := json.Unmarshal([]byte(`["READY","INDEXING","REPLICATION_FINALIZING","REPLICATION_HYDRATING","REPLICATION_DEHYDRATING"]`), &res); err != nil {
		panic(err)
	}
	for _, v := range res {
		getReplicationStatusReplicaRequestOKBodyTypeStatusPropEnum = append(getReplicationStatusReplicaRequestOKBodyTypeStatusPropEnum, v)
	}
}

const (

	// GetReplicationStatusReplicaRequestOKBodyStatusREADY captures enum value "READY"
	GetReplicationStatusReplicaRequestOKBodyStatusREADY string = "READY"

	// GetReplicationStatusReplicaRequestOKBodyStatusINDEXING captures enum value "INDEXING"
	GetReplicationStatusReplicaRequestOKBodyStatusINDEXING string = "INDEXING"

	// GetReplicationStatusReplicaRequestOKBodyStatusREPLICATIONFINALIZING captures enum value "REPLICATION_FINALIZING"
	GetReplicationStatusReplicaRequestOKBodyStatusREPLICATIONFINALIZING string = "REPLICATION_FINALIZING"

	// GetReplicationStatusReplicaRequestOKBodyStatusREPLICATIONHYDRATING captures enum value "REPLICATION_HYDRATING"
	GetReplicationStatusReplicaRequestOKBodyStatusREPLICATIONHYDRATING string = "REPLICATION_HYDRATING"

	// GetReplicationStatusReplicaRequestOKBodyStatusREPLICATIONDEHYDRATING captures enum value "REPLICATION_DEHYDRATING"
	GetReplicationStatusReplicaRequestOKBodyStatusREPLICATIONDEHYDRATING string = "REPLICATION_DEHYDRATING"
)

// prop value enum
func (o *GetReplicationStatusReplicaRequestOKBody) validateStatusEnum(path, location string, value string) error {
	if err := validate.EnumCase(path, location, value, getReplicationStatusReplicaRequestOKBodyTypeStatusPropEnum, true); err != nil {
		return err
	}
	return nil
}

func (o *GetReplicationStatusReplicaRequestOKBody) validateStatus(formats strfmt.Registry) error {

	if err := validate.Required("getReplicationStatusReplicaRequestOK"+"."+"status", "body", o.Status); err != nil {
		return err
	}

	// value enum
	if err := o.validateStatusEnum("getReplicationStatusReplicaRequestOK"+"."+"status", "body", *o.Status); err != nil {
		return err
	}

	return nil
}

// ContextValidate validates this get replication status replica request o k body based on context it is used
func (o *GetReplicationStatusReplicaRequestOKBody) ContextValidate(ctx context.Context, formats strfmt.Registry) error {
	return nil
}

// MarshalBinary interface implementation
func (o *GetReplicationStatusReplicaRequestOKBody) MarshalBinary() ([]byte, error) {
	if o == nil {
		return nil, nil
	}
	return swag.WriteJSON(o)
}

// UnmarshalBinary interface implementation
func (o *GetReplicationStatusReplicaRequestOKBody) UnmarshalBinary(b []byte) error {
	var res GetReplicationStatusReplicaRequestOKBody
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*o = res
	return nil
}
