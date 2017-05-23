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

// WeaviateModelManifestsUpdateOKCode is the HTTP code returned for type WeaviateModelManifestsUpdateOK
const WeaviateModelManifestsUpdateOKCode int = 200

/*WeaviateModelManifestsUpdateOK Successful updated.

swagger:response weaviateModelManifestsUpdateOK
*/
type WeaviateModelManifestsUpdateOK struct {

	/*
	  In: Body
	*/
	Payload *models.ModelManifest `json:"body,omitempty"`
}

// NewWeaviateModelManifestsUpdateOK creates WeaviateModelManifestsUpdateOK with default headers values
func NewWeaviateModelManifestsUpdateOK() *WeaviateModelManifestsUpdateOK {
	return &WeaviateModelManifestsUpdateOK{}
}

// WithPayload adds the payload to the weaviate model manifests update o k response
func (o *WeaviateModelManifestsUpdateOK) WithPayload(payload *models.ModelManifest) *WeaviateModelManifestsUpdateOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate model manifests update o k response
func (o *WeaviateModelManifestsUpdateOK) SetPayload(payload *models.ModelManifest) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateModelManifestsUpdateOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateModelManifestsUpdateUnauthorizedCode is the HTTP code returned for type WeaviateModelManifestsUpdateUnauthorized
const WeaviateModelManifestsUpdateUnauthorizedCode int = 401

/*WeaviateModelManifestsUpdateUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateModelManifestsUpdateUnauthorized
*/
type WeaviateModelManifestsUpdateUnauthorized struct {
}

// NewWeaviateModelManifestsUpdateUnauthorized creates WeaviateModelManifestsUpdateUnauthorized with default headers values
func NewWeaviateModelManifestsUpdateUnauthorized() *WeaviateModelManifestsUpdateUnauthorized {
	return &WeaviateModelManifestsUpdateUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateModelManifestsUpdateUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateModelManifestsUpdateForbiddenCode is the HTTP code returned for type WeaviateModelManifestsUpdateForbidden
const WeaviateModelManifestsUpdateForbiddenCode int = 403

/*WeaviateModelManifestsUpdateForbidden The used API-key has insufficient permissions.

swagger:response weaviateModelManifestsUpdateForbidden
*/
type WeaviateModelManifestsUpdateForbidden struct {
}

// NewWeaviateModelManifestsUpdateForbidden creates WeaviateModelManifestsUpdateForbidden with default headers values
func NewWeaviateModelManifestsUpdateForbidden() *WeaviateModelManifestsUpdateForbidden {
	return &WeaviateModelManifestsUpdateForbidden{}
}

// WriteResponse to the client
func (o *WeaviateModelManifestsUpdateForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateModelManifestsUpdateNotFoundCode is the HTTP code returned for type WeaviateModelManifestsUpdateNotFound
const WeaviateModelManifestsUpdateNotFoundCode int = 404

/*WeaviateModelManifestsUpdateNotFound Successful query result but no resource was found.

swagger:response weaviateModelManifestsUpdateNotFound
*/
type WeaviateModelManifestsUpdateNotFound struct {
}

// NewWeaviateModelManifestsUpdateNotFound creates WeaviateModelManifestsUpdateNotFound with default headers values
func NewWeaviateModelManifestsUpdateNotFound() *WeaviateModelManifestsUpdateNotFound {
	return &WeaviateModelManifestsUpdateNotFound{}
}

// WriteResponse to the client
func (o *WeaviateModelManifestsUpdateNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateModelManifestsUpdateNotImplementedCode is the HTTP code returned for type WeaviateModelManifestsUpdateNotImplemented
const WeaviateModelManifestsUpdateNotImplementedCode int = 501

/*WeaviateModelManifestsUpdateNotImplemented Not (yet) implemented.

swagger:response weaviateModelManifestsUpdateNotImplemented
*/
type WeaviateModelManifestsUpdateNotImplemented struct {
}

// NewWeaviateModelManifestsUpdateNotImplemented creates WeaviateModelManifestsUpdateNotImplemented with default headers values
func NewWeaviateModelManifestsUpdateNotImplemented() *WeaviateModelManifestsUpdateNotImplemented {
	return &WeaviateModelManifestsUpdateNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateModelManifestsUpdateNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
