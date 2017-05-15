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
 package model_manifests




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/models"
)

// WeaviateModelManifestsListOKCode is the HTTP code returned for type WeaviateModelManifestsListOK
const WeaviateModelManifestsListOKCode int = 200

/*WeaviateModelManifestsListOK Successful response.

swagger:response weaviateModelManifestsListOK
*/
type WeaviateModelManifestsListOK struct {

	/*
	  In: Body
	*/
	Payload *models.ModelManifestsListResponse `json:"body,omitempty"`
}

// NewWeaviateModelManifestsListOK creates WeaviateModelManifestsListOK with default headers values
func NewWeaviateModelManifestsListOK() *WeaviateModelManifestsListOK {
	return &WeaviateModelManifestsListOK{}
}

// WithPayload adds the payload to the weaviate model manifests list o k response
func (o *WeaviateModelManifestsListOK) WithPayload(payload *models.ModelManifestsListResponse) *WeaviateModelManifestsListOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate model manifests list o k response
func (o *WeaviateModelManifestsListOK) SetPayload(payload *models.ModelManifestsListResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateModelManifestsListOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateModelManifestsListUnauthorizedCode is the HTTP code returned for type WeaviateModelManifestsListUnauthorized
const WeaviateModelManifestsListUnauthorizedCode int = 401

/*WeaviateModelManifestsListUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateModelManifestsListUnauthorized
*/
type WeaviateModelManifestsListUnauthorized struct {
}

// NewWeaviateModelManifestsListUnauthorized creates WeaviateModelManifestsListUnauthorized with default headers values
func NewWeaviateModelManifestsListUnauthorized() *WeaviateModelManifestsListUnauthorized {
	return &WeaviateModelManifestsListUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateModelManifestsListUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateModelManifestsListForbiddenCode is the HTTP code returned for type WeaviateModelManifestsListForbidden
const WeaviateModelManifestsListForbiddenCode int = 403

/*WeaviateModelManifestsListForbidden The used API-key has insufficient permissions.

swagger:response weaviateModelManifestsListForbidden
*/
type WeaviateModelManifestsListForbidden struct {
}

// NewWeaviateModelManifestsListForbidden creates WeaviateModelManifestsListForbidden with default headers values
func NewWeaviateModelManifestsListForbidden() *WeaviateModelManifestsListForbidden {
	return &WeaviateModelManifestsListForbidden{}
}

// WriteResponse to the client
func (o *WeaviateModelManifestsListForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateModelManifestsListNotFoundCode is the HTTP code returned for type WeaviateModelManifestsListNotFound
const WeaviateModelManifestsListNotFoundCode int = 404

/*WeaviateModelManifestsListNotFound Successful query result but no resource was found.

swagger:response weaviateModelManifestsListNotFound
*/
type WeaviateModelManifestsListNotFound struct {
}

// NewWeaviateModelManifestsListNotFound creates WeaviateModelManifestsListNotFound with default headers values
func NewWeaviateModelManifestsListNotFound() *WeaviateModelManifestsListNotFound {
	return &WeaviateModelManifestsListNotFound{}
}

// WriteResponse to the client
func (o *WeaviateModelManifestsListNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateModelManifestsListNotImplementedCode is the HTTP code returned for type WeaviateModelManifestsListNotImplemented
const WeaviateModelManifestsListNotImplementedCode int = 501

/*WeaviateModelManifestsListNotImplemented Not (yet) implemented.

swagger:response weaviateModelManifestsListNotImplemented
*/
type WeaviateModelManifestsListNotImplemented struct {
}

// NewWeaviateModelManifestsListNotImplemented creates WeaviateModelManifestsListNotImplemented with default headers values
func NewWeaviateModelManifestsListNotImplemented() *WeaviateModelManifestsListNotImplemented {
	return &WeaviateModelManifestsListNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateModelManifestsListNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
