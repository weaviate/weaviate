/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * Contact: @CreativeSofwFdn / yourfriends@weaviate.com
 */

package keys

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/creativesoftwarefdn/weaviate/models"
)

// WeaviateKeysMeGetRenewOKCode is the HTTP code returned for type WeaviateKeysMeGetRenewOK
const WeaviateKeysMeGetRenewOKCode int = 200

/*WeaviateKeysMeGetRenewOK Successful response.

swagger:response weaviateKeysMeGetRenewOK
*/
type WeaviateKeysMeGetRenewOK struct {

	/*
	  In: Body
	*/
	Payload *models.KeyTokenGetResponse `json:"body,omitempty"`
}

// NewWeaviateKeysMeGetRenewOK creates WeaviateKeysMeGetRenewOK with default headers values
func NewWeaviateKeysMeGetRenewOK() *WeaviateKeysMeGetRenewOK {
	return &WeaviateKeysMeGetRenewOK{}
}

// WithPayload adds the payload to the weaviate keys me get renew o k response
func (o *WeaviateKeysMeGetRenewOK) WithPayload(payload *models.KeyTokenGetResponse) *WeaviateKeysMeGetRenewOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate keys me get renew o k response
func (o *WeaviateKeysMeGetRenewOK) SetPayload(payload *models.KeyTokenGetResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateKeysMeGetRenewOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateKeysMeGetRenewUnauthorizedCode is the HTTP code returned for type WeaviateKeysMeGetRenewUnauthorized
const WeaviateKeysMeGetRenewUnauthorizedCode int = 401

/*WeaviateKeysMeGetRenewUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateKeysMeGetRenewUnauthorized
*/
type WeaviateKeysMeGetRenewUnauthorized struct {
}

// NewWeaviateKeysMeGetRenewUnauthorized creates WeaviateKeysMeGetRenewUnauthorized with default headers values
func NewWeaviateKeysMeGetRenewUnauthorized() *WeaviateKeysMeGetRenewUnauthorized {
	return &WeaviateKeysMeGetRenewUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateKeysMeGetRenewUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateKeysMeGetRenewForbiddenCode is the HTTP code returned for type WeaviateKeysMeGetRenewForbidden
const WeaviateKeysMeGetRenewForbiddenCode int = 403

/*WeaviateKeysMeGetRenewForbidden The used API-key has insufficient permissions.

swagger:response weaviateKeysMeGetRenewForbidden
*/
type WeaviateKeysMeGetRenewForbidden struct {
}

// NewWeaviateKeysMeGetRenewForbidden creates WeaviateKeysMeGetRenewForbidden with default headers values
func NewWeaviateKeysMeGetRenewForbidden() *WeaviateKeysMeGetRenewForbidden {
	return &WeaviateKeysMeGetRenewForbidden{}
}

// WriteResponse to the client
func (o *WeaviateKeysMeGetRenewForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateKeysMeGetRenewNotFoundCode is the HTTP code returned for type WeaviateKeysMeGetRenewNotFound
const WeaviateKeysMeGetRenewNotFoundCode int = 404

/*WeaviateKeysMeGetRenewNotFound Successful query result but no resource was found.

swagger:response weaviateKeysMeGetRenewNotFound
*/
type WeaviateKeysMeGetRenewNotFound struct {
}

// NewWeaviateKeysMeGetRenewNotFound creates WeaviateKeysMeGetRenewNotFound with default headers values
func NewWeaviateKeysMeGetRenewNotFound() *WeaviateKeysMeGetRenewNotFound {
	return &WeaviateKeysMeGetRenewNotFound{}
}

// WriteResponse to the client
func (o *WeaviateKeysMeGetRenewNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateKeysMeGetRenewUnprocessableEntityCode is the HTTP code returned for type WeaviateKeysMeGetRenewUnprocessableEntity
const WeaviateKeysMeGetRenewUnprocessableEntityCode int = 422

/*WeaviateKeysMeGetRenewUnprocessableEntity Request body contains well-formed (i.e., syntactically correct), but semantically erroneous. Are you sure the class is defined in the configuration file?

swagger:response weaviateKeysMeGetRenewUnprocessableEntity
*/
type WeaviateKeysMeGetRenewUnprocessableEntity struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewWeaviateKeysMeGetRenewUnprocessableEntity creates WeaviateKeysMeGetRenewUnprocessableEntity with default headers values
func NewWeaviateKeysMeGetRenewUnprocessableEntity() *WeaviateKeysMeGetRenewUnprocessableEntity {
	return &WeaviateKeysMeGetRenewUnprocessableEntity{}
}

// WithPayload adds the payload to the weaviate keys me get renew unprocessable entity response
func (o *WeaviateKeysMeGetRenewUnprocessableEntity) WithPayload(payload *models.ErrorResponse) *WeaviateKeysMeGetRenewUnprocessableEntity {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate keys me get renew unprocessable entity response
func (o *WeaviateKeysMeGetRenewUnprocessableEntity) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateKeysMeGetRenewUnprocessableEntity) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(422)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateKeysMeGetRenewNotImplementedCode is the HTTP code returned for type WeaviateKeysMeGetRenewNotImplemented
const WeaviateKeysMeGetRenewNotImplementedCode int = 501

/*WeaviateKeysMeGetRenewNotImplemented Not (yet) implemented.

swagger:response weaviateKeysMeGetRenewNotImplemented
*/
type WeaviateKeysMeGetRenewNotImplemented struct {
}

// NewWeaviateKeysMeGetRenewNotImplemented creates WeaviateKeysMeGetRenewNotImplemented with default headers values
func NewWeaviateKeysMeGetRenewNotImplemented() *WeaviateKeysMeGetRenewNotImplemented {
	return &WeaviateKeysMeGetRenewNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateKeysMeGetRenewNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
