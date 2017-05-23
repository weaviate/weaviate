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

// WeaviateModelManifestsValidateThingStateCreatedCode is the HTTP code returned for type WeaviateModelManifestsValidateThingStateCreated
const WeaviateModelManifestsValidateThingStateCreatedCode int = 201

/*WeaviateModelManifestsValidateThingStateCreated Successful created.

swagger:response weaviateModelManifestsValidateThingStateCreated
*/
type WeaviateModelManifestsValidateThingStateCreated struct {

	/*
	  In: Body
	*/
	Payload *models.ModelManifestsValidateThingStateResponse `json:"body,omitempty"`
}

// NewWeaviateModelManifestsValidateThingStateCreated creates WeaviateModelManifestsValidateThingStateCreated with default headers values
func NewWeaviateModelManifestsValidateThingStateCreated() *WeaviateModelManifestsValidateThingStateCreated {
	return &WeaviateModelManifestsValidateThingStateCreated{}
}

// WithPayload adds the payload to the weaviate model manifests validate thing state created response
func (o *WeaviateModelManifestsValidateThingStateCreated) WithPayload(payload *models.ModelManifestsValidateThingStateResponse) *WeaviateModelManifestsValidateThingStateCreated {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate model manifests validate thing state created response
func (o *WeaviateModelManifestsValidateThingStateCreated) SetPayload(payload *models.ModelManifestsValidateThingStateResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateModelManifestsValidateThingStateCreated) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(201)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateModelManifestsValidateThingStateUnauthorizedCode is the HTTP code returned for type WeaviateModelManifestsValidateThingStateUnauthorized
const WeaviateModelManifestsValidateThingStateUnauthorizedCode int = 401

/*WeaviateModelManifestsValidateThingStateUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateModelManifestsValidateThingStateUnauthorized
*/
type WeaviateModelManifestsValidateThingStateUnauthorized struct {
}

// NewWeaviateModelManifestsValidateThingStateUnauthorized creates WeaviateModelManifestsValidateThingStateUnauthorized with default headers values
func NewWeaviateModelManifestsValidateThingStateUnauthorized() *WeaviateModelManifestsValidateThingStateUnauthorized {
	return &WeaviateModelManifestsValidateThingStateUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateModelManifestsValidateThingStateUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateModelManifestsValidateThingStateForbiddenCode is the HTTP code returned for type WeaviateModelManifestsValidateThingStateForbidden
const WeaviateModelManifestsValidateThingStateForbiddenCode int = 403

/*WeaviateModelManifestsValidateThingStateForbidden The used API-key has insufficient permissions.

swagger:response weaviateModelManifestsValidateThingStateForbidden
*/
type WeaviateModelManifestsValidateThingStateForbidden struct {
}

// NewWeaviateModelManifestsValidateThingStateForbidden creates WeaviateModelManifestsValidateThingStateForbidden with default headers values
func NewWeaviateModelManifestsValidateThingStateForbidden() *WeaviateModelManifestsValidateThingStateForbidden {
	return &WeaviateModelManifestsValidateThingStateForbidden{}
}

// WriteResponse to the client
func (o *WeaviateModelManifestsValidateThingStateForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateModelManifestsValidateThingStateNotImplementedCode is the HTTP code returned for type WeaviateModelManifestsValidateThingStateNotImplemented
const WeaviateModelManifestsValidateThingStateNotImplementedCode int = 501

/*WeaviateModelManifestsValidateThingStateNotImplemented Not (yet) implemented.

swagger:response weaviateModelManifestsValidateThingStateNotImplemented
*/
type WeaviateModelManifestsValidateThingStateNotImplemented struct {
}

// NewWeaviateModelManifestsValidateThingStateNotImplemented creates WeaviateModelManifestsValidateThingStateNotImplemented with default headers values
func NewWeaviateModelManifestsValidateThingStateNotImplemented() *WeaviateModelManifestsValidateThingStateNotImplemented {
	return &WeaviateModelManifestsValidateThingStateNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateModelManifestsValidateThingStateNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
