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
   

package actions

 
 

import (
	"net/http"

	"github.com/go-openapi/runtime"
)

// WeaviateActionsValidateOKCode is the HTTP code returned for type WeaviateActionsValidateOK
const WeaviateActionsValidateOKCode int = 200

/*WeaviateActionsValidateOK Successful validated.

swagger:response weaviateActionsValidateOK
*/
type WeaviateActionsValidateOK struct {
}

// NewWeaviateActionsValidateOK creates WeaviateActionsValidateOK with default headers values
func NewWeaviateActionsValidateOK() *WeaviateActionsValidateOK {
	return &WeaviateActionsValidateOK{}
}

// WriteResponse to the client
func (o *WeaviateActionsValidateOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
}

// WeaviateActionsValidateUnauthorizedCode is the HTTP code returned for type WeaviateActionsValidateUnauthorized
const WeaviateActionsValidateUnauthorizedCode int = 401

/*WeaviateActionsValidateUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateActionsValidateUnauthorized
*/
type WeaviateActionsValidateUnauthorized struct {
}

// NewWeaviateActionsValidateUnauthorized creates WeaviateActionsValidateUnauthorized with default headers values
func NewWeaviateActionsValidateUnauthorized() *WeaviateActionsValidateUnauthorized {
	return &WeaviateActionsValidateUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateActionsValidateUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateActionsValidateForbiddenCode is the HTTP code returned for type WeaviateActionsValidateForbidden
const WeaviateActionsValidateForbiddenCode int = 403

/*WeaviateActionsValidateForbidden The used API-key has insufficient permissions.

swagger:response weaviateActionsValidateForbidden
*/
type WeaviateActionsValidateForbidden struct {
}

// NewWeaviateActionsValidateForbidden creates WeaviateActionsValidateForbidden with default headers values
func NewWeaviateActionsValidateForbidden() *WeaviateActionsValidateForbidden {
	return &WeaviateActionsValidateForbidden{}
}

// WriteResponse to the client
func (o *WeaviateActionsValidateForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateActionsValidateUnprocessableEntityCode is the HTTP code returned for type WeaviateActionsValidateUnprocessableEntity
const WeaviateActionsValidateUnprocessableEntityCode int = 422

/*WeaviateActionsValidateUnprocessableEntity Request body contains well-formed (i.e., syntactically correct), but semantically erroneous. Are you sure the class is defined in the configuration file?

swagger:response weaviateActionsValidateUnprocessableEntity
*/
type WeaviateActionsValidateUnprocessableEntity struct {
}

// NewWeaviateActionsValidateUnprocessableEntity creates WeaviateActionsValidateUnprocessableEntity with default headers values
func NewWeaviateActionsValidateUnprocessableEntity() *WeaviateActionsValidateUnprocessableEntity {
	return &WeaviateActionsValidateUnprocessableEntity{}
}

// WriteResponse to the client
func (o *WeaviateActionsValidateUnprocessableEntity) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(422)
}

// WeaviateActionsValidateNotImplementedCode is the HTTP code returned for type WeaviateActionsValidateNotImplemented
const WeaviateActionsValidateNotImplementedCode int = 501

/*WeaviateActionsValidateNotImplemented Not (yet) implemented.

swagger:response weaviateActionsValidateNotImplemented
*/
type WeaviateActionsValidateNotImplemented struct {
}

// NewWeaviateActionsValidateNotImplemented creates WeaviateActionsValidateNotImplemented with default headers values
func NewWeaviateActionsValidateNotImplemented() *WeaviateActionsValidateNotImplemented {
	return &WeaviateActionsValidateNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateActionsValidateNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
