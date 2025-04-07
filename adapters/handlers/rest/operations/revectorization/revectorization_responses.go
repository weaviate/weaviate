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

package revectorization

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/entities/models"
)

// RevectorizationOKCode is the HTTP code returned for type RevectorizationOK
const RevectorizationOKCode int = 200

/*
RevectorizationOK Revectorization process successfully started

swagger:response revectorizationOK
*/
type RevectorizationOK struct {

	/*
	  In: Body
	*/
	Payload *models.RevectorizationStatusResponse `json:"body,omitempty"`
}

// NewRevectorizationOK creates RevectorizationOK with default headers values
func NewRevectorizationOK() *RevectorizationOK {

	return &RevectorizationOK{}
}

// WithPayload adds the payload to the revectorization o k response
func (o *RevectorizationOK) WithPayload(payload *models.RevectorizationStatusResponse) *RevectorizationOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the revectorization o k response
func (o *RevectorizationOK) SetPayload(payload *models.RevectorizationStatusResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *RevectorizationOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
