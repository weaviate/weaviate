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

// WeaviateModelManifestsValidateComponentsCreatedCode is the HTTP code returned for type WeaviateModelManifestsValidateComponentsCreated
const WeaviateModelManifestsValidateComponentsCreatedCode int = 201

/*WeaviateModelManifestsValidateComponentsCreated Successful created.

swagger:response weaviateModelManifestsValidateComponentsCreated
*/
type WeaviateModelManifestsValidateComponentsCreated struct {

	/*
	  In: Body
	*/
	Payload *models.ModelManifestsValidateComponentsResponse `json:"body,omitempty"`
}

// NewWeaviateModelManifestsValidateComponentsCreated creates WeaviateModelManifestsValidateComponentsCreated with default headers values
func NewWeaviateModelManifestsValidateComponentsCreated() *WeaviateModelManifestsValidateComponentsCreated {
	return &WeaviateModelManifestsValidateComponentsCreated{}
}

// WithPayload adds the payload to the weaviate model manifests validate components created response
func (o *WeaviateModelManifestsValidateComponentsCreated) WithPayload(payload *models.ModelManifestsValidateComponentsResponse) *WeaviateModelManifestsValidateComponentsCreated {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate model manifests validate components created response
func (o *WeaviateModelManifestsValidateComponentsCreated) SetPayload(payload *models.ModelManifestsValidateComponentsResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateModelManifestsValidateComponentsCreated) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(201)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateModelManifestsValidateComponentsUnauthorizedCode is the HTTP code returned for type WeaviateModelManifestsValidateComponentsUnauthorized
const WeaviateModelManifestsValidateComponentsUnauthorizedCode int = 401

/*WeaviateModelManifestsValidateComponentsUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateModelManifestsValidateComponentsUnauthorized
*/
type WeaviateModelManifestsValidateComponentsUnauthorized struct {
}

// NewWeaviateModelManifestsValidateComponentsUnauthorized creates WeaviateModelManifestsValidateComponentsUnauthorized with default headers values
func NewWeaviateModelManifestsValidateComponentsUnauthorized() *WeaviateModelManifestsValidateComponentsUnauthorized {
	return &WeaviateModelManifestsValidateComponentsUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateModelManifestsValidateComponentsUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateModelManifestsValidateComponentsForbiddenCode is the HTTP code returned for type WeaviateModelManifestsValidateComponentsForbidden
const WeaviateModelManifestsValidateComponentsForbiddenCode int = 403

/*WeaviateModelManifestsValidateComponentsForbidden The used API-key has insufficient permissions.

swagger:response weaviateModelManifestsValidateComponentsForbidden
*/
type WeaviateModelManifestsValidateComponentsForbidden struct {
}

// NewWeaviateModelManifestsValidateComponentsForbidden creates WeaviateModelManifestsValidateComponentsForbidden with default headers values
func NewWeaviateModelManifestsValidateComponentsForbidden() *WeaviateModelManifestsValidateComponentsForbidden {
	return &WeaviateModelManifestsValidateComponentsForbidden{}
}

// WriteResponse to the client
func (o *WeaviateModelManifestsValidateComponentsForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateModelManifestsValidateComponentsNotImplementedCode is the HTTP code returned for type WeaviateModelManifestsValidateComponentsNotImplemented
const WeaviateModelManifestsValidateComponentsNotImplementedCode int = 501

/*WeaviateModelManifestsValidateComponentsNotImplemented Not (yet) implemented.

swagger:response weaviateModelManifestsValidateComponentsNotImplemented
*/
type WeaviateModelManifestsValidateComponentsNotImplemented struct {
}

// NewWeaviateModelManifestsValidateComponentsNotImplemented creates WeaviateModelManifestsValidateComponentsNotImplemented with default headers values
func NewWeaviateModelManifestsValidateComponentsNotImplemented() *WeaviateModelManifestsValidateComponentsNotImplemented {
	return &WeaviateModelManifestsValidateComponentsNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateModelManifestsValidateComponentsNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
