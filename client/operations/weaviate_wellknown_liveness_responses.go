// Code generated by go-swagger; DO NOT EDIT.

package operations

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"fmt"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"
)

// WeaviateWellknownLivenessReader is a Reader for the WeaviateWellknownLiveness structure.
type WeaviateWellknownLivenessReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *WeaviateWellknownLivenessReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewWeaviateWellknownLivenessOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		return nil, runtime.NewAPIError("response status code does not match any response statuses defined for this endpoint in the swagger spec", response, response.Code())
	}
}

// NewWeaviateWellknownLivenessOK creates a WeaviateWellknownLivenessOK with default headers values
func NewWeaviateWellknownLivenessOK() *WeaviateWellknownLivenessOK {
	return &WeaviateWellknownLivenessOK{}
}

/*
WeaviateWellknownLivenessOK describes a response with status code 200, with default header values.

The application is able to respond to HTTP requests
*/
type WeaviateWellknownLivenessOK struct {
}

// IsSuccess returns true when this weaviate wellknown liveness o k response has a 2xx status code
func (o *WeaviateWellknownLivenessOK) IsSuccess() bool {
	return true
}

// IsRedirect returns true when this weaviate wellknown liveness o k response has a 3xx status code
func (o *WeaviateWellknownLivenessOK) IsRedirect() bool {
	return false
}

// IsClientError returns true when this weaviate wellknown liveness o k response has a 4xx status code
func (o *WeaviateWellknownLivenessOK) IsClientError() bool {
	return false
}

// IsServerError returns true when this weaviate wellknown liveness o k response has a 5xx status code
func (o *WeaviateWellknownLivenessOK) IsServerError() bool {
	return false
}

// IsCode returns true when this weaviate wellknown liveness o k response a status code equal to that given
func (o *WeaviateWellknownLivenessOK) IsCode(code int) bool {
	return code == 200
}

// Code gets the status code for the weaviate wellknown liveness o k response
func (o *WeaviateWellknownLivenessOK) Code() int {
	return 200
}

func (o *WeaviateWellknownLivenessOK) Error() string {
	return fmt.Sprintf("[GET /.well-known/live][%d] weaviateWellknownLivenessOK ", 200)
}

func (o *WeaviateWellknownLivenessOK) String() string {
	return fmt.Sprintf("[GET /.well-known/live][%d] weaviateWellknownLivenessOK ", 200)
}

func (o *WeaviateWellknownLivenessOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}
