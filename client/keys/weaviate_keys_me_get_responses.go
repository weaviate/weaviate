// Code generated by go-swagger; DO NOT EDIT.

package keys

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"fmt"
	"io"

	"github.com/go-openapi/runtime"

	strfmt "github.com/go-openapi/strfmt"

	models "github.com/creativesoftwarefdn/weaviate/models"
)

// WeaviateKeysMeGetReader is a Reader for the WeaviateKeysMeGet structure.
type WeaviateKeysMeGetReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *WeaviateKeysMeGetReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {

	case 200:
		result := NewWeaviateKeysMeGetOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil

	case 401:
		result := NewWeaviateKeysMeGetUnauthorized()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result

	case 404:
		result := NewWeaviateKeysMeGetNotFound()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result

	case 501:
		result := NewWeaviateKeysMeGetNotImplemented()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result

	default:
		return nil, runtime.NewAPIError("unknown error", response, response.Code())
	}
}

// NewWeaviateKeysMeGetOK creates a WeaviateKeysMeGetOK with default headers values
func NewWeaviateKeysMeGetOK() *WeaviateKeysMeGetOK {
	return &WeaviateKeysMeGetOK{}
}

/*WeaviateKeysMeGetOK handles this case with default header values.

Successful response.
*/
type WeaviateKeysMeGetOK struct {
	Payload *models.KeyGetResponse
}

func (o *WeaviateKeysMeGetOK) Error() string {
	return fmt.Sprintf("[GET /keys/me][%d] weaviateKeysMeGetOK  %+v", 200, o.Payload)
}

func (o *WeaviateKeysMeGetOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.KeyGetResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewWeaviateKeysMeGetUnauthorized creates a WeaviateKeysMeGetUnauthorized with default headers values
func NewWeaviateKeysMeGetUnauthorized() *WeaviateKeysMeGetUnauthorized {
	return &WeaviateKeysMeGetUnauthorized{}
}

/*WeaviateKeysMeGetUnauthorized handles this case with default header values.

Unauthorized or invalid credentials.
*/
type WeaviateKeysMeGetUnauthorized struct {
}

func (o *WeaviateKeysMeGetUnauthorized) Error() string {
	return fmt.Sprintf("[GET /keys/me][%d] weaviateKeysMeGetUnauthorized ", 401)
}

func (o *WeaviateKeysMeGetUnauthorized) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewWeaviateKeysMeGetNotFound creates a WeaviateKeysMeGetNotFound with default headers values
func NewWeaviateKeysMeGetNotFound() *WeaviateKeysMeGetNotFound {
	return &WeaviateKeysMeGetNotFound{}
}

/*WeaviateKeysMeGetNotFound handles this case with default header values.

Successful query result but no resource was found.
*/
type WeaviateKeysMeGetNotFound struct {
}

func (o *WeaviateKeysMeGetNotFound) Error() string {
	return fmt.Sprintf("[GET /keys/me][%d] weaviateKeysMeGetNotFound ", 404)
}

func (o *WeaviateKeysMeGetNotFound) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewWeaviateKeysMeGetNotImplemented creates a WeaviateKeysMeGetNotImplemented with default headers values
func NewWeaviateKeysMeGetNotImplemented() *WeaviateKeysMeGetNotImplemented {
	return &WeaviateKeysMeGetNotImplemented{}
}

/*WeaviateKeysMeGetNotImplemented handles this case with default header values.

Not (yet) implemented.
*/
type WeaviateKeysMeGetNotImplemented struct {
}

func (o *WeaviateKeysMeGetNotImplemented) Error() string {
	return fmt.Sprintf("[GET /keys/me][%d] weaviateKeysMeGetNotImplemented ", 501)
}

func (o *WeaviateKeysMeGetNotImplemented) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}
