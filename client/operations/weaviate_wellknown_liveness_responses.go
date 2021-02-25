//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

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
		return nil, runtime.NewAPIError("unknown error", response, response.Code())
	}
}

// NewWeaviateWellknownLivenessOK creates a WeaviateWellknownLivenessOK with default headers values
func NewWeaviateWellknownLivenessOK() *WeaviateWellknownLivenessOK {
	return &WeaviateWellknownLivenessOK{}
}

/*WeaviateWellknownLivenessOK handles this case with default header values.

The application is able to respond to HTTP requests
*/
type WeaviateWellknownLivenessOK struct {
}

func (o *WeaviateWellknownLivenessOK) Error() string {
	return fmt.Sprintf("[GET /.well-known/live][%d] weaviateWellknownLivenessOK ", 200)
}

func (o *WeaviateWellknownLivenessOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}
