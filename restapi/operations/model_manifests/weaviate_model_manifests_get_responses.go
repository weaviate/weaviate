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

// WeaviateModelManifestsGetOKCode is the HTTP code returned for type WeaviateModelManifestsGetOK
const WeaviateModelManifestsGetOKCode int = 200

/*WeaviateModelManifestsGetOK Successful response.

swagger:response weaviateModelManifestsGetOK
*/
type WeaviateModelManifestsGetOK struct {

	/*
	  In: Body
	*/
	Payload *models.ModelManifest `json:"body,omitempty"`
}

// NewWeaviateModelManifestsGetOK creates WeaviateModelManifestsGetOK with default headers values
func NewWeaviateModelManifestsGetOK() *WeaviateModelManifestsGetOK {
	return &WeaviateModelManifestsGetOK{}
}

// WithPayload adds the payload to the weaviate model manifests get o k response
func (o *WeaviateModelManifestsGetOK) WithPayload(payload *models.ModelManifest) *WeaviateModelManifestsGetOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate model manifests get o k response
func (o *WeaviateModelManifestsGetOK) SetPayload(payload *models.ModelManifest) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateModelManifestsGetOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateModelManifestsGetUnauthorizedCode is the HTTP code returned for type WeaviateModelManifestsGetUnauthorized
const WeaviateModelManifestsGetUnauthorizedCode int = 401

/*WeaviateModelManifestsGetUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateModelManifestsGetUnauthorized
*/
type WeaviateModelManifestsGetUnauthorized struct {
}

// NewWeaviateModelManifestsGetUnauthorized creates WeaviateModelManifestsGetUnauthorized with default headers values
func NewWeaviateModelManifestsGetUnauthorized() *WeaviateModelManifestsGetUnauthorized {
	return &WeaviateModelManifestsGetUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateModelManifestsGetUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateModelManifestsGetForbiddenCode is the HTTP code returned for type WeaviateModelManifestsGetForbidden
const WeaviateModelManifestsGetForbiddenCode int = 403

/*WeaviateModelManifestsGetForbidden The used API-key has insufficient permissions.

swagger:response weaviateModelManifestsGetForbidden
*/
type WeaviateModelManifestsGetForbidden struct {
}

// NewWeaviateModelManifestsGetForbidden creates WeaviateModelManifestsGetForbidden with default headers values
func NewWeaviateModelManifestsGetForbidden() *WeaviateModelManifestsGetForbidden {
	return &WeaviateModelManifestsGetForbidden{}
}

// WriteResponse to the client
func (o *WeaviateModelManifestsGetForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateModelManifestsGetNotFoundCode is the HTTP code returned for type WeaviateModelManifestsGetNotFound
const WeaviateModelManifestsGetNotFoundCode int = 404

/*WeaviateModelManifestsGetNotFound Successful query result but no resource was found.

swagger:response weaviateModelManifestsGetNotFound
*/
type WeaviateModelManifestsGetNotFound struct {
}

// NewWeaviateModelManifestsGetNotFound creates WeaviateModelManifestsGetNotFound with default headers values
func NewWeaviateModelManifestsGetNotFound() *WeaviateModelManifestsGetNotFound {
	return &WeaviateModelManifestsGetNotFound{}
}

// WriteResponse to the client
func (o *WeaviateModelManifestsGetNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateModelManifestsGetNotImplementedCode is the HTTP code returned for type WeaviateModelManifestsGetNotImplemented
const WeaviateModelManifestsGetNotImplementedCode int = 501

/*WeaviateModelManifestsGetNotImplemented Not (yet) implemented.

swagger:response weaviateModelManifestsGetNotImplemented
*/
type WeaviateModelManifestsGetNotImplemented struct {
}

// NewWeaviateModelManifestsGetNotImplemented creates WeaviateModelManifestsGetNotImplemented with default headers values
func NewWeaviateModelManifestsGetNotImplemented() *WeaviateModelManifestsGetNotImplemented {
	return &WeaviateModelManifestsGetNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateModelManifestsGetNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
