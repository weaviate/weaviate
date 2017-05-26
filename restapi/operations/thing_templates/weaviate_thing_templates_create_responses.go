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
 package thing_templates




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/models"
)

// WeaviateThingTemplatesCreateAcceptedCode is the HTTP code returned for type WeaviateThingTemplatesCreateAccepted
const WeaviateThingTemplatesCreateAcceptedCode int = 202

/*WeaviateThingTemplatesCreateAccepted Successfully received.

swagger:response weaviateThingTemplatesCreateAccepted
*/
type WeaviateThingTemplatesCreateAccepted struct {

	/*
	  In: Body
	*/
	Payload *models.ThingTemplateGetResponse `json:"body,omitempty"`
}

// NewWeaviateThingTemplatesCreateAccepted creates WeaviateThingTemplatesCreateAccepted with default headers values
func NewWeaviateThingTemplatesCreateAccepted() *WeaviateThingTemplatesCreateAccepted {
	return &WeaviateThingTemplatesCreateAccepted{}
}

// WithPayload adds the payload to the weaviate thing templates create accepted response
func (o *WeaviateThingTemplatesCreateAccepted) WithPayload(payload *models.ThingTemplateGetResponse) *WeaviateThingTemplatesCreateAccepted {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate thing templates create accepted response
func (o *WeaviateThingTemplatesCreateAccepted) SetPayload(payload *models.ThingTemplateGetResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateThingTemplatesCreateAccepted) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(202)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateThingTemplatesCreateUnauthorizedCode is the HTTP code returned for type WeaviateThingTemplatesCreateUnauthorized
const WeaviateThingTemplatesCreateUnauthorizedCode int = 401

/*WeaviateThingTemplatesCreateUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateThingTemplatesCreateUnauthorized
*/
type WeaviateThingTemplatesCreateUnauthorized struct {
}

// NewWeaviateThingTemplatesCreateUnauthorized creates WeaviateThingTemplatesCreateUnauthorized with default headers values
func NewWeaviateThingTemplatesCreateUnauthorized() *WeaviateThingTemplatesCreateUnauthorized {
	return &WeaviateThingTemplatesCreateUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateThingTemplatesCreateUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateThingTemplatesCreateForbiddenCode is the HTTP code returned for type WeaviateThingTemplatesCreateForbidden
const WeaviateThingTemplatesCreateForbiddenCode int = 403

/*WeaviateThingTemplatesCreateForbidden The used API-key has insufficient permissions.

swagger:response weaviateThingTemplatesCreateForbidden
*/
type WeaviateThingTemplatesCreateForbidden struct {
}

// NewWeaviateThingTemplatesCreateForbidden creates WeaviateThingTemplatesCreateForbidden with default headers values
func NewWeaviateThingTemplatesCreateForbidden() *WeaviateThingTemplatesCreateForbidden {
	return &WeaviateThingTemplatesCreateForbidden{}
}

// WriteResponse to the client
func (o *WeaviateThingTemplatesCreateForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateThingTemplatesCreateNotImplementedCode is the HTTP code returned for type WeaviateThingTemplatesCreateNotImplemented
const WeaviateThingTemplatesCreateNotImplementedCode int = 501

/*WeaviateThingTemplatesCreateNotImplemented Not (yet) implemented.

swagger:response weaviateThingTemplatesCreateNotImplemented
*/
type WeaviateThingTemplatesCreateNotImplemented struct {
}

// NewWeaviateThingTemplatesCreateNotImplemented creates WeaviateThingTemplatesCreateNotImplemented with default headers values
func NewWeaviateThingTemplatesCreateNotImplemented() *WeaviateThingTemplatesCreateNotImplemented {
	return &WeaviateThingTemplatesCreateNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateThingTemplatesCreateNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
