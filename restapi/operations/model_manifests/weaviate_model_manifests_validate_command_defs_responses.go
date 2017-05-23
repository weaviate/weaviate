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

// WeaviateModelManifestsValidateCommandDefsCreatedCode is the HTTP code returned for type WeaviateModelManifestsValidateCommandDefsCreated
const WeaviateModelManifestsValidateCommandDefsCreatedCode int = 201

/*WeaviateModelManifestsValidateCommandDefsCreated Successful created.

swagger:response weaviateModelManifestsValidateCommandDefsCreated
*/
type WeaviateModelManifestsValidateCommandDefsCreated struct {

	/*
	  In: Body
	*/
	Payload *models.ModelManifestsValidateCommandDefsResponse `json:"body,omitempty"`
}

// NewWeaviateModelManifestsValidateCommandDefsCreated creates WeaviateModelManifestsValidateCommandDefsCreated with default headers values
func NewWeaviateModelManifestsValidateCommandDefsCreated() *WeaviateModelManifestsValidateCommandDefsCreated {
	return &WeaviateModelManifestsValidateCommandDefsCreated{}
}

// WithPayload adds the payload to the weaviate model manifests validate command defs created response
func (o *WeaviateModelManifestsValidateCommandDefsCreated) WithPayload(payload *models.ModelManifestsValidateCommandDefsResponse) *WeaviateModelManifestsValidateCommandDefsCreated {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate model manifests validate command defs created response
func (o *WeaviateModelManifestsValidateCommandDefsCreated) SetPayload(payload *models.ModelManifestsValidateCommandDefsResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateModelManifestsValidateCommandDefsCreated) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(201)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateModelManifestsValidateCommandDefsUnauthorizedCode is the HTTP code returned for type WeaviateModelManifestsValidateCommandDefsUnauthorized
const WeaviateModelManifestsValidateCommandDefsUnauthorizedCode int = 401

/*WeaviateModelManifestsValidateCommandDefsUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateModelManifestsValidateCommandDefsUnauthorized
*/
type WeaviateModelManifestsValidateCommandDefsUnauthorized struct {
}

// NewWeaviateModelManifestsValidateCommandDefsUnauthorized creates WeaviateModelManifestsValidateCommandDefsUnauthorized with default headers values
func NewWeaviateModelManifestsValidateCommandDefsUnauthorized() *WeaviateModelManifestsValidateCommandDefsUnauthorized {
	return &WeaviateModelManifestsValidateCommandDefsUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateModelManifestsValidateCommandDefsUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateModelManifestsValidateCommandDefsForbiddenCode is the HTTP code returned for type WeaviateModelManifestsValidateCommandDefsForbidden
const WeaviateModelManifestsValidateCommandDefsForbiddenCode int = 403

/*WeaviateModelManifestsValidateCommandDefsForbidden The used API-key has insufficient permissions.

swagger:response weaviateModelManifestsValidateCommandDefsForbidden
*/
type WeaviateModelManifestsValidateCommandDefsForbidden struct {
}

// NewWeaviateModelManifestsValidateCommandDefsForbidden creates WeaviateModelManifestsValidateCommandDefsForbidden with default headers values
func NewWeaviateModelManifestsValidateCommandDefsForbidden() *WeaviateModelManifestsValidateCommandDefsForbidden {
	return &WeaviateModelManifestsValidateCommandDefsForbidden{}
}

// WriteResponse to the client
func (o *WeaviateModelManifestsValidateCommandDefsForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateModelManifestsValidateCommandDefsNotImplementedCode is the HTTP code returned for type WeaviateModelManifestsValidateCommandDefsNotImplemented
const WeaviateModelManifestsValidateCommandDefsNotImplementedCode int = 501

/*WeaviateModelManifestsValidateCommandDefsNotImplemented Not (yet) implemented.

swagger:response weaviateModelManifestsValidateCommandDefsNotImplemented
*/
type WeaviateModelManifestsValidateCommandDefsNotImplemented struct {
}

// NewWeaviateModelManifestsValidateCommandDefsNotImplemented creates WeaviateModelManifestsValidateCommandDefsNotImplemented with default headers values
func NewWeaviateModelManifestsValidateCommandDefsNotImplemented() *WeaviateModelManifestsValidateCommandDefsNotImplemented {
	return &WeaviateModelManifestsValidateCommandDefsNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateModelManifestsValidateCommandDefsNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
