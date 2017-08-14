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

// WeaviateThingTemplatesGetOKCode is the HTTP code returned for type WeaviateThingTemplatesGetOK
const WeaviateThingTemplatesGetOKCode int = 200

/*WeaviateThingTemplatesGetOK Successful response.

swagger:response weaviateThingTemplatesGetOK
*/
type WeaviateThingTemplatesGetOK struct {

	/*
	  In: Body
	*/
	Payload *models.ThingTemplateGetResponse `json:"body,omitempty"`
}

// NewWeaviateThingTemplatesGetOK creates WeaviateThingTemplatesGetOK with default headers values
func NewWeaviateThingTemplatesGetOK() *WeaviateThingTemplatesGetOK {
	return &WeaviateThingTemplatesGetOK{}
}

// WithPayload adds the payload to the weaviate thing templates get o k response
func (o *WeaviateThingTemplatesGetOK) WithPayload(payload *models.ThingTemplateGetResponse) *WeaviateThingTemplatesGetOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate thing templates get o k response
func (o *WeaviateThingTemplatesGetOK) SetPayload(payload *models.ThingTemplateGetResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateThingTemplatesGetOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateThingTemplatesGetUnauthorizedCode is the HTTP code returned for type WeaviateThingTemplatesGetUnauthorized
const WeaviateThingTemplatesGetUnauthorizedCode int = 401

/*WeaviateThingTemplatesGetUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateThingTemplatesGetUnauthorized
*/
type WeaviateThingTemplatesGetUnauthorized struct {
}

// NewWeaviateThingTemplatesGetUnauthorized creates WeaviateThingTemplatesGetUnauthorized with default headers values
func NewWeaviateThingTemplatesGetUnauthorized() *WeaviateThingTemplatesGetUnauthorized {
	return &WeaviateThingTemplatesGetUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateThingTemplatesGetUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateThingTemplatesGetForbiddenCode is the HTTP code returned for type WeaviateThingTemplatesGetForbidden
const WeaviateThingTemplatesGetForbiddenCode int = 403

/*WeaviateThingTemplatesGetForbidden The used API-key has insufficient permissions.

swagger:response weaviateThingTemplatesGetForbidden
*/
type WeaviateThingTemplatesGetForbidden struct {
}

// NewWeaviateThingTemplatesGetForbidden creates WeaviateThingTemplatesGetForbidden with default headers values
func NewWeaviateThingTemplatesGetForbidden() *WeaviateThingTemplatesGetForbidden {
	return &WeaviateThingTemplatesGetForbidden{}
}

// WriteResponse to the client
func (o *WeaviateThingTemplatesGetForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateThingTemplatesGetNotFoundCode is the HTTP code returned for type WeaviateThingTemplatesGetNotFound
const WeaviateThingTemplatesGetNotFoundCode int = 404

/*WeaviateThingTemplatesGetNotFound Successful query result but no resource was found.

swagger:response weaviateThingTemplatesGetNotFound
*/
type WeaviateThingTemplatesGetNotFound struct {
}

// NewWeaviateThingTemplatesGetNotFound creates WeaviateThingTemplatesGetNotFound with default headers values
func NewWeaviateThingTemplatesGetNotFound() *WeaviateThingTemplatesGetNotFound {
	return &WeaviateThingTemplatesGetNotFound{}
}

// WriteResponse to the client
func (o *WeaviateThingTemplatesGetNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateThingTemplatesGetNotImplementedCode is the HTTP code returned for type WeaviateThingTemplatesGetNotImplemented
const WeaviateThingTemplatesGetNotImplementedCode int = 501

/*WeaviateThingTemplatesGetNotImplemented Not (yet) implemented.

swagger:response weaviateThingTemplatesGetNotImplemented
*/
type WeaviateThingTemplatesGetNotImplemented struct {
}

// NewWeaviateThingTemplatesGetNotImplemented creates WeaviateThingTemplatesGetNotImplemented with default headers values
func NewWeaviateThingTemplatesGetNotImplemented() *WeaviateThingTemplatesGetNotImplemented {
	return &WeaviateThingTemplatesGetNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateThingTemplatesGetNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
