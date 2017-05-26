/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 Weaviate. All rights reserved.
 * LICENSE: https://github.com/weaviate/weaviate/blob/master/LICENSE
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */
 package keys




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/models"
)

// WeaviateKeyCreateAcceptedCode is the HTTP code returned for type WeaviateKeyCreateAccepted
const WeaviateKeyCreateAcceptedCode int = 202

/*WeaviateKeyCreateAccepted Successfully received.

swagger:response weaviateKeyCreateAccepted
*/
type WeaviateKeyCreateAccepted struct {

	/*
	  In: Body
	*/
	Payload *models.KeyGetResponse `json:"body,omitempty"`
}

// NewWeaviateKeyCreateAccepted creates WeaviateKeyCreateAccepted with default headers values
func NewWeaviateKeyCreateAccepted() *WeaviateKeyCreateAccepted {
	return &WeaviateKeyCreateAccepted{}
}

// WithPayload adds the payload to the weaviate key create accepted response
func (o *WeaviateKeyCreateAccepted) WithPayload(payload *models.KeyGetResponse) *WeaviateKeyCreateAccepted {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate key create accepted response
func (o *WeaviateKeyCreateAccepted) SetPayload(payload *models.KeyGetResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateKeyCreateAccepted) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(202)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateKeyCreateUnauthorizedCode is the HTTP code returned for type WeaviateKeyCreateUnauthorized
const WeaviateKeyCreateUnauthorizedCode int = 401

/*WeaviateKeyCreateUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateKeyCreateUnauthorized
*/
type WeaviateKeyCreateUnauthorized struct {
}

// NewWeaviateKeyCreateUnauthorized creates WeaviateKeyCreateUnauthorized with default headers values
func NewWeaviateKeyCreateUnauthorized() *WeaviateKeyCreateUnauthorized {
	return &WeaviateKeyCreateUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateKeyCreateUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateKeyCreateForbiddenCode is the HTTP code returned for type WeaviateKeyCreateForbidden
const WeaviateKeyCreateForbiddenCode int = 403

/*WeaviateKeyCreateForbidden The used API-key has insufficient permissions.

swagger:response weaviateKeyCreateForbidden
*/
type WeaviateKeyCreateForbidden struct {
}

// NewWeaviateKeyCreateForbidden creates WeaviateKeyCreateForbidden with default headers values
func NewWeaviateKeyCreateForbidden() *WeaviateKeyCreateForbidden {
	return &WeaviateKeyCreateForbidden{}
}

// WriteResponse to the client
func (o *WeaviateKeyCreateForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateKeyCreateNotImplementedCode is the HTTP code returned for type WeaviateKeyCreateNotImplemented
const WeaviateKeyCreateNotImplementedCode int = 501

/*WeaviateKeyCreateNotImplemented Not (yet) implemented.

swagger:response weaviateKeyCreateNotImplemented
*/
type WeaviateKeyCreateNotImplemented struct {
}

// NewWeaviateKeyCreateNotImplemented creates WeaviateKeyCreateNotImplemented with default headers values
func NewWeaviateKeyCreateNotImplemented() *WeaviateKeyCreateNotImplemented {
	return &WeaviateKeyCreateNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateKeyCreateNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
