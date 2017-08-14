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

// WeaviateThingTemplatesListOKCode is the HTTP code returned for type WeaviateThingTemplatesListOK
const WeaviateThingTemplatesListOKCode int = 200

/*WeaviateThingTemplatesListOK Successful response.

swagger:response weaviateThingTemplatesListOK
*/
type WeaviateThingTemplatesListOK struct {

	/*
	  In: Body
	*/
	Payload *models.ThingTemplatesListResponse `json:"body,omitempty"`
}

// NewWeaviateThingTemplatesListOK creates WeaviateThingTemplatesListOK with default headers values
func NewWeaviateThingTemplatesListOK() *WeaviateThingTemplatesListOK {
	return &WeaviateThingTemplatesListOK{}
}

// WithPayload adds the payload to the weaviate thing templates list o k response
func (o *WeaviateThingTemplatesListOK) WithPayload(payload *models.ThingTemplatesListResponse) *WeaviateThingTemplatesListOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate thing templates list o k response
func (o *WeaviateThingTemplatesListOK) SetPayload(payload *models.ThingTemplatesListResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateThingTemplatesListOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateThingTemplatesListUnauthorizedCode is the HTTP code returned for type WeaviateThingTemplatesListUnauthorized
const WeaviateThingTemplatesListUnauthorizedCode int = 401

/*WeaviateThingTemplatesListUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateThingTemplatesListUnauthorized
*/
type WeaviateThingTemplatesListUnauthorized struct {
}

// NewWeaviateThingTemplatesListUnauthorized creates WeaviateThingTemplatesListUnauthorized with default headers values
func NewWeaviateThingTemplatesListUnauthorized() *WeaviateThingTemplatesListUnauthorized {
	return &WeaviateThingTemplatesListUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateThingTemplatesListUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateThingTemplatesListForbiddenCode is the HTTP code returned for type WeaviateThingTemplatesListForbidden
const WeaviateThingTemplatesListForbiddenCode int = 403

/*WeaviateThingTemplatesListForbidden The used API-key has insufficient permissions.

swagger:response weaviateThingTemplatesListForbidden
*/
type WeaviateThingTemplatesListForbidden struct {
}

// NewWeaviateThingTemplatesListForbidden creates WeaviateThingTemplatesListForbidden with default headers values
func NewWeaviateThingTemplatesListForbidden() *WeaviateThingTemplatesListForbidden {
	return &WeaviateThingTemplatesListForbidden{}
}

// WriteResponse to the client
func (o *WeaviateThingTemplatesListForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateThingTemplatesListNotFoundCode is the HTTP code returned for type WeaviateThingTemplatesListNotFound
const WeaviateThingTemplatesListNotFoundCode int = 404

/*WeaviateThingTemplatesListNotFound Successful query result but no resource was found.

swagger:response weaviateThingTemplatesListNotFound
*/
type WeaviateThingTemplatesListNotFound struct {
}

// NewWeaviateThingTemplatesListNotFound creates WeaviateThingTemplatesListNotFound with default headers values
func NewWeaviateThingTemplatesListNotFound() *WeaviateThingTemplatesListNotFound {
	return &WeaviateThingTemplatesListNotFound{}
}

// WriteResponse to the client
func (o *WeaviateThingTemplatesListNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateThingTemplatesListNotImplementedCode is the HTTP code returned for type WeaviateThingTemplatesListNotImplemented
const WeaviateThingTemplatesListNotImplementedCode int = 501

/*WeaviateThingTemplatesListNotImplemented Not (yet) implemented.

swagger:response weaviateThingTemplatesListNotImplemented
*/
type WeaviateThingTemplatesListNotImplemented struct {
}

// NewWeaviateThingTemplatesListNotImplemented creates WeaviateThingTemplatesListNotImplemented with default headers values
func NewWeaviateThingTemplatesListNotImplemented() *WeaviateThingTemplatesListNotImplemented {
	return &WeaviateThingTemplatesListNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateThingTemplatesListNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
