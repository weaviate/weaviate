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

// WeaviateModelManifestsPatchOKCode is the HTTP code returned for type WeaviateModelManifestsPatchOK
const WeaviateModelManifestsPatchOKCode int = 200

/*WeaviateModelManifestsPatchOK Successful updated.

swagger:response weaviateModelManifestsPatchOK
*/
type WeaviateModelManifestsPatchOK struct {

	/*
	  In: Body
	*/
	Payload *models.ModelManifest `json:"body,omitempty"`
}

// NewWeaviateModelManifestsPatchOK creates WeaviateModelManifestsPatchOK with default headers values
func NewWeaviateModelManifestsPatchOK() *WeaviateModelManifestsPatchOK {
	return &WeaviateModelManifestsPatchOK{}
}

// WithPayload adds the payload to the weaviate model manifests patch o k response
func (o *WeaviateModelManifestsPatchOK) WithPayload(payload *models.ModelManifest) *WeaviateModelManifestsPatchOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate model manifests patch o k response
func (o *WeaviateModelManifestsPatchOK) SetPayload(payload *models.ModelManifest) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateModelManifestsPatchOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateModelManifestsPatchBadRequestCode is the HTTP code returned for type WeaviateModelManifestsPatchBadRequest
const WeaviateModelManifestsPatchBadRequestCode int = 400

/*WeaviateModelManifestsPatchBadRequest The patch-JSON is malformed.

swagger:response weaviateModelManifestsPatchBadRequest
*/
type WeaviateModelManifestsPatchBadRequest struct {
}

// NewWeaviateModelManifestsPatchBadRequest creates WeaviateModelManifestsPatchBadRequest with default headers values
func NewWeaviateModelManifestsPatchBadRequest() *WeaviateModelManifestsPatchBadRequest {
	return &WeaviateModelManifestsPatchBadRequest{}
}

// WriteResponse to the client
func (o *WeaviateModelManifestsPatchBadRequest) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(400)
}

// WeaviateModelManifestsPatchUnauthorizedCode is the HTTP code returned for type WeaviateModelManifestsPatchUnauthorized
const WeaviateModelManifestsPatchUnauthorizedCode int = 401

/*WeaviateModelManifestsPatchUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateModelManifestsPatchUnauthorized
*/
type WeaviateModelManifestsPatchUnauthorized struct {
}

// NewWeaviateModelManifestsPatchUnauthorized creates WeaviateModelManifestsPatchUnauthorized with default headers values
func NewWeaviateModelManifestsPatchUnauthorized() *WeaviateModelManifestsPatchUnauthorized {
	return &WeaviateModelManifestsPatchUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateModelManifestsPatchUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateModelManifestsPatchForbiddenCode is the HTTP code returned for type WeaviateModelManifestsPatchForbidden
const WeaviateModelManifestsPatchForbiddenCode int = 403

/*WeaviateModelManifestsPatchForbidden The used API-key has insufficient permissions.

swagger:response weaviateModelManifestsPatchForbidden
*/
type WeaviateModelManifestsPatchForbidden struct {
}

// NewWeaviateModelManifestsPatchForbidden creates WeaviateModelManifestsPatchForbidden with default headers values
func NewWeaviateModelManifestsPatchForbidden() *WeaviateModelManifestsPatchForbidden {
	return &WeaviateModelManifestsPatchForbidden{}
}

// WriteResponse to the client
func (o *WeaviateModelManifestsPatchForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateModelManifestsPatchNotFoundCode is the HTTP code returned for type WeaviateModelManifestsPatchNotFound
const WeaviateModelManifestsPatchNotFoundCode int = 404

/*WeaviateModelManifestsPatchNotFound Successful query result but no resource was found.

swagger:response weaviateModelManifestsPatchNotFound
*/
type WeaviateModelManifestsPatchNotFound struct {
}

// NewWeaviateModelManifestsPatchNotFound creates WeaviateModelManifestsPatchNotFound with default headers values
func NewWeaviateModelManifestsPatchNotFound() *WeaviateModelManifestsPatchNotFound {
	return &WeaviateModelManifestsPatchNotFound{}
}

// WriteResponse to the client
func (o *WeaviateModelManifestsPatchNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateModelManifestsPatchUnprocessableEntityCode is the HTTP code returned for type WeaviateModelManifestsPatchUnprocessableEntity
const WeaviateModelManifestsPatchUnprocessableEntityCode int = 422

/*WeaviateModelManifestsPatchUnprocessableEntity The patch-JSON is valid but unprocessable.

swagger:response weaviateModelManifestsPatchUnprocessableEntity
*/
type WeaviateModelManifestsPatchUnprocessableEntity struct {
}

// NewWeaviateModelManifestsPatchUnprocessableEntity creates WeaviateModelManifestsPatchUnprocessableEntity with default headers values
func NewWeaviateModelManifestsPatchUnprocessableEntity() *WeaviateModelManifestsPatchUnprocessableEntity {
	return &WeaviateModelManifestsPatchUnprocessableEntity{}
}

// WriteResponse to the client
func (o *WeaviateModelManifestsPatchUnprocessableEntity) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(422)
}

// WeaviateModelManifestsPatchNotImplementedCode is the HTTP code returned for type WeaviateModelManifestsPatchNotImplemented
const WeaviateModelManifestsPatchNotImplementedCode int = 501

/*WeaviateModelManifestsPatchNotImplemented Not (yet) implemented.

swagger:response weaviateModelManifestsPatchNotImplemented
*/
type WeaviateModelManifestsPatchNotImplemented struct {
}

// NewWeaviateModelManifestsPatchNotImplemented creates WeaviateModelManifestsPatchNotImplemented with default headers values
func NewWeaviateModelManifestsPatchNotImplemented() *WeaviateModelManifestsPatchNotImplemented {
	return &WeaviateModelManifestsPatchNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateModelManifestsPatchNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
