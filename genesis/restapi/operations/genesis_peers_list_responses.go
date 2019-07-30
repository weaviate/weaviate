//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

// Code generated by go-swagger; DO NOT EDIT.

package operations

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	models "github.com/semi-technologies/weaviate/genesis/models"
)

// GenesisPeersListOKCode is the HTTP code returned for type GenesisPeersListOK
const GenesisPeersListOKCode int = 200

/*GenesisPeersListOK The list of registered peers

swagger:response genesisPeersListOK
*/
type GenesisPeersListOK struct {

	/*
	  In: Body
	*/
	Payload []*models.Peer `json:"body,omitempty"`
}

// NewGenesisPeersListOK creates GenesisPeersListOK with default headers values
func NewGenesisPeersListOK() *GenesisPeersListOK {

	return &GenesisPeersListOK{}
}

// WithPayload adds the payload to the genesis peers list o k response
func (o *GenesisPeersListOK) WithPayload(payload []*models.Peer) *GenesisPeersListOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the genesis peers list o k response
func (o *GenesisPeersListOK) SetPayload(payload []*models.Peer) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *GenesisPeersListOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	payload := o.Payload
	if payload == nil {
		payload = make([]*models.Peer, 0, 50)
	}

	if err := producer.Produce(rw, payload); err != nil {
		panic(err) // let the recovery middleware deal with this
	}

}

// GenesisPeersListInternalServerErrorCode is the HTTP code returned for type GenesisPeersListInternalServerError
const GenesisPeersListInternalServerErrorCode int = 500

/*GenesisPeersListInternalServerError Internal error

swagger:response genesisPeersListInternalServerError
*/
type GenesisPeersListInternalServerError struct {
}

// NewGenesisPeersListInternalServerError creates GenesisPeersListInternalServerError with default headers values
func NewGenesisPeersListInternalServerError() *GenesisPeersListInternalServerError {

	return &GenesisPeersListInternalServerError{}
}

// WriteResponse to the client
func (o *GenesisPeersListInternalServerError) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(500)
}
