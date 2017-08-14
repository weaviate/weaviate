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
   

package groups

 
 

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/models"
)

// WeaviateGroupsCreateAcceptedCode is the HTTP code returned for type WeaviateGroupsCreateAccepted
const WeaviateGroupsCreateAcceptedCode int = 202

/*WeaviateGroupsCreateAccepted Successfully received.

swagger:response weaviateGroupsCreateAccepted
*/
type WeaviateGroupsCreateAccepted struct {

	/*
	  In: Body
	*/
	Payload *models.GroupGetResponse `json:"body,omitempty"`
}

// NewWeaviateGroupsCreateAccepted creates WeaviateGroupsCreateAccepted with default headers values
func NewWeaviateGroupsCreateAccepted() *WeaviateGroupsCreateAccepted {
	return &WeaviateGroupsCreateAccepted{}
}

// WithPayload adds the payload to the weaviate groups create accepted response
func (o *WeaviateGroupsCreateAccepted) WithPayload(payload *models.GroupGetResponse) *WeaviateGroupsCreateAccepted {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate groups create accepted response
func (o *WeaviateGroupsCreateAccepted) SetPayload(payload *models.GroupGetResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateGroupsCreateAccepted) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(202)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateGroupsCreateUnauthorizedCode is the HTTP code returned for type WeaviateGroupsCreateUnauthorized
const WeaviateGroupsCreateUnauthorizedCode int = 401

/*WeaviateGroupsCreateUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateGroupsCreateUnauthorized
*/
type WeaviateGroupsCreateUnauthorized struct {
}

// NewWeaviateGroupsCreateUnauthorized creates WeaviateGroupsCreateUnauthorized with default headers values
func NewWeaviateGroupsCreateUnauthorized() *WeaviateGroupsCreateUnauthorized {
	return &WeaviateGroupsCreateUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateGroupsCreateUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateGroupsCreateForbiddenCode is the HTTP code returned for type WeaviateGroupsCreateForbidden
const WeaviateGroupsCreateForbiddenCode int = 403

/*WeaviateGroupsCreateForbidden The used API-key has insufficient permissions.

swagger:response weaviateGroupsCreateForbidden
*/
type WeaviateGroupsCreateForbidden struct {
}

// NewWeaviateGroupsCreateForbidden creates WeaviateGroupsCreateForbidden with default headers values
func NewWeaviateGroupsCreateForbidden() *WeaviateGroupsCreateForbidden {
	return &WeaviateGroupsCreateForbidden{}
}

// WriteResponse to the client
func (o *WeaviateGroupsCreateForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateGroupsCreateUnprocessableEntityCode is the HTTP code returned for type WeaviateGroupsCreateUnprocessableEntity
const WeaviateGroupsCreateUnprocessableEntityCode int = 422

/*WeaviateGroupsCreateUnprocessableEntity Can not validate, check the body.

swagger:response weaviateGroupsCreateUnprocessableEntity
*/
type WeaviateGroupsCreateUnprocessableEntity struct {
}

// NewWeaviateGroupsCreateUnprocessableEntity creates WeaviateGroupsCreateUnprocessableEntity with default headers values
func NewWeaviateGroupsCreateUnprocessableEntity() *WeaviateGroupsCreateUnprocessableEntity {
	return &WeaviateGroupsCreateUnprocessableEntity{}
}

// WriteResponse to the client
func (o *WeaviateGroupsCreateUnprocessableEntity) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(422)
}

// WeaviateGroupsCreateNotImplementedCode is the HTTP code returned for type WeaviateGroupsCreateNotImplemented
const WeaviateGroupsCreateNotImplementedCode int = 501

/*WeaviateGroupsCreateNotImplemented Not (yet) implemented.

swagger:response weaviateGroupsCreateNotImplemented
*/
type WeaviateGroupsCreateNotImplemented struct {
}

// NewWeaviateGroupsCreateNotImplemented creates WeaviateGroupsCreateNotImplemented with default headers values
func NewWeaviateGroupsCreateNotImplemented() *WeaviateGroupsCreateNotImplemented {
	return &WeaviateGroupsCreateNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateGroupsCreateNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
