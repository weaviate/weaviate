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

// GenesisPeersLeaveReader is a Reader for the GenesisPeersLeave structure.
type GenesisPeersLeaveReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *GenesisPeersLeaveReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 204:
		result := NewGenesisPeersLeaveNoContent()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	case 401:
		result := NewGenesisPeersLeaveUnauthorized()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 403:
		result := NewGenesisPeersLeaveForbidden()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result
	case 404:
		result := NewGenesisPeersLeaveNotFound()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result

	default:
		return nil, runtime.NewAPIError("unknown error", response, response.Code())
	}
}

// NewGenesisPeersLeaveNoContent creates a GenesisPeersLeaveNoContent with default headers values
func NewGenesisPeersLeaveNoContent() *GenesisPeersLeaveNoContent {
	return &GenesisPeersLeaveNoContent{}
}

/*GenesisPeersLeaveNoContent handles this case with default header values.

Successful left the network.
*/
type GenesisPeersLeaveNoContent struct {
}

func (o *GenesisPeersLeaveNoContent) Error() string {
	return fmt.Sprintf("[DELETE /peers/{peerId}][%d] genesisPeersLeaveNoContent ", 204)
}

func (o *GenesisPeersLeaveNoContent) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {
	return nil
}

// NewGenesisPeersLeaveUnauthorized creates a GenesisPeersLeaveUnauthorized with default headers values
func NewGenesisPeersLeaveUnauthorized() *GenesisPeersLeaveUnauthorized {
	return &GenesisPeersLeaveUnauthorized{}
}

/*GenesisPeersLeaveUnauthorized handles this case with default header values.

Unauthorized or invalid credentials.
*/
type GenesisPeersLeaveUnauthorized struct {
}

func (o *GenesisPeersLeaveUnauthorized) Error() string {
	return fmt.Sprintf("[DELETE /peers/{peerId}][%d] genesisPeersLeaveUnauthorized ", 401)
}

func (o *GenesisPeersLeaveUnauthorized) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {
	return nil
}

// NewGenesisPeersLeaveForbidden creates a GenesisPeersLeaveForbidden with default headers values
func NewGenesisPeersLeaveForbidden() *GenesisPeersLeaveForbidden {
	return &GenesisPeersLeaveForbidden{}
}

/*GenesisPeersLeaveForbidden handles this case with default header values.

The used API-key has insufficient permissions.
*/
type GenesisPeersLeaveForbidden struct {
}

func (o *GenesisPeersLeaveForbidden) Error() string {
	return fmt.Sprintf("[DELETE /peers/{peerId}][%d] genesisPeersLeaveForbidden ", 403)
}

func (o *GenesisPeersLeaveForbidden) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {
	return nil
}

// NewGenesisPeersLeaveNotFound creates a GenesisPeersLeaveNotFound with default headers values
func NewGenesisPeersLeaveNotFound() *GenesisPeersLeaveNotFound {
	return &GenesisPeersLeaveNotFound{}
}

/*GenesisPeersLeaveNotFound handles this case with default header values.

Successful query result but no such peer was found.
*/
type GenesisPeersLeaveNotFound struct {
}

func (o *GenesisPeersLeaveNotFound) Error() string {
	return fmt.Sprintf("[DELETE /peers/{peerId}][%d] genesisPeersLeaveNotFound ", 404)
}

func (o *GenesisPeersLeaveNotFound) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {
	return nil
}
