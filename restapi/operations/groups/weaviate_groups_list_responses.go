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

// WeaviateGroupsListOKCode is the HTTP code returned for type WeaviateGroupsListOK
const WeaviateGroupsListOKCode int = 200

/*WeaviateGroupsListOK Successful response.

swagger:response weaviateGroupsListOK
*/
type WeaviateGroupsListOK struct {

	/*
	  In: Body
	*/
	Payload *models.GroupsListResponse `json:"body,omitempty"`
}

// NewWeaviateGroupsListOK creates WeaviateGroupsListOK with default headers values
func NewWeaviateGroupsListOK() *WeaviateGroupsListOK {
	return &WeaviateGroupsListOK{}
}

// WithPayload adds the payload to the weaviate groups list o k response
func (o *WeaviateGroupsListOK) WithPayload(payload *models.GroupsListResponse) *WeaviateGroupsListOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate groups list o k response
func (o *WeaviateGroupsListOK) SetPayload(payload *models.GroupsListResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateGroupsListOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateGroupsListUnauthorizedCode is the HTTP code returned for type WeaviateGroupsListUnauthorized
const WeaviateGroupsListUnauthorizedCode int = 401

/*WeaviateGroupsListUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateGroupsListUnauthorized
*/
type WeaviateGroupsListUnauthorized struct {
}

// NewWeaviateGroupsListUnauthorized creates WeaviateGroupsListUnauthorized with default headers values
func NewWeaviateGroupsListUnauthorized() *WeaviateGroupsListUnauthorized {
	return &WeaviateGroupsListUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateGroupsListUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateGroupsListForbiddenCode is the HTTP code returned for type WeaviateGroupsListForbidden
const WeaviateGroupsListForbiddenCode int = 403

/*WeaviateGroupsListForbidden The used API-key has insufficient permissions.

swagger:response weaviateGroupsListForbidden
*/
type WeaviateGroupsListForbidden struct {
}

// NewWeaviateGroupsListForbidden creates WeaviateGroupsListForbidden with default headers values
func NewWeaviateGroupsListForbidden() *WeaviateGroupsListForbidden {
	return &WeaviateGroupsListForbidden{}
}

// WriteResponse to the client
func (o *WeaviateGroupsListForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateGroupsListNotFoundCode is the HTTP code returned for type WeaviateGroupsListNotFound
const WeaviateGroupsListNotFoundCode int = 404

/*WeaviateGroupsListNotFound Successful query result but no resource was found.

swagger:response weaviateGroupsListNotFound
*/
type WeaviateGroupsListNotFound struct {
}

// NewWeaviateGroupsListNotFound creates WeaviateGroupsListNotFound with default headers values
func NewWeaviateGroupsListNotFound() *WeaviateGroupsListNotFound {
	return &WeaviateGroupsListNotFound{}
}

// WriteResponse to the client
func (o *WeaviateGroupsListNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateGroupsListNotImplementedCode is the HTTP code returned for type WeaviateGroupsListNotImplemented
const WeaviateGroupsListNotImplementedCode int = 501

/*WeaviateGroupsListNotImplemented Not (yet) implemented.

swagger:response weaviateGroupsListNotImplemented
*/
type WeaviateGroupsListNotImplemented struct {
}

// NewWeaviateGroupsListNotImplemented creates WeaviateGroupsListNotImplemented with default headers values
func NewWeaviateGroupsListNotImplemented() *WeaviateGroupsListNotImplemented {
	return &WeaviateGroupsListNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateGroupsListNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
