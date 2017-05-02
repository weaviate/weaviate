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

// WeaviateModelManifestsGetNotFoundCode is the HTTP code returned for type WeaviateModelManifestsGetNotFound
const WeaviateModelManifestsGetNotFoundCode int = 404

/*WeaviateModelManifestsGetNotFound Successful query result but no resource was found

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
