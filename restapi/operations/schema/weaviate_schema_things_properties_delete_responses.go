/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN: Bob van Luijt (bob@k10y.co)
 */
// Code generated by go-swagger; DO NOT EDIT.

package schema

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"
)

// WeaviateSchemaThingsPropertiesDeleteOKCode is the HTTP code returned for type WeaviateSchemaThingsPropertiesDeleteOK
const WeaviateSchemaThingsPropertiesDeleteOKCode int = 200

/*WeaviateSchemaThingsPropertiesDeleteOK Removed the property from the ontology.

swagger:response weaviateSchemaThingsPropertiesDeleteOK
*/
type WeaviateSchemaThingsPropertiesDeleteOK struct {
}

// NewWeaviateSchemaThingsPropertiesDeleteOK creates WeaviateSchemaThingsPropertiesDeleteOK with default headers values
func NewWeaviateSchemaThingsPropertiesDeleteOK() *WeaviateSchemaThingsPropertiesDeleteOK {

	return &WeaviateSchemaThingsPropertiesDeleteOK{}
}

// WriteResponse to the client
func (o *WeaviateSchemaThingsPropertiesDeleteOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(200)
}

// WeaviateSchemaThingsPropertiesDeleteUnauthorizedCode is the HTTP code returned for type WeaviateSchemaThingsPropertiesDeleteUnauthorized
const WeaviateSchemaThingsPropertiesDeleteUnauthorizedCode int = 401

/*WeaviateSchemaThingsPropertiesDeleteUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateSchemaThingsPropertiesDeleteUnauthorized
*/
type WeaviateSchemaThingsPropertiesDeleteUnauthorized struct {
}

// NewWeaviateSchemaThingsPropertiesDeleteUnauthorized creates WeaviateSchemaThingsPropertiesDeleteUnauthorized with default headers values
func NewWeaviateSchemaThingsPropertiesDeleteUnauthorized() *WeaviateSchemaThingsPropertiesDeleteUnauthorized {

	return &WeaviateSchemaThingsPropertiesDeleteUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateSchemaThingsPropertiesDeleteUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(401)
}

// WeaviateSchemaThingsPropertiesDeleteForbiddenCode is the HTTP code returned for type WeaviateSchemaThingsPropertiesDeleteForbidden
const WeaviateSchemaThingsPropertiesDeleteForbiddenCode int = 403

/*WeaviateSchemaThingsPropertiesDeleteForbidden Could not find the Thing class or property.

swagger:response weaviateSchemaThingsPropertiesDeleteForbidden
*/
type WeaviateSchemaThingsPropertiesDeleteForbidden struct {
}

// NewWeaviateSchemaThingsPropertiesDeleteForbidden creates WeaviateSchemaThingsPropertiesDeleteForbidden with default headers values
func NewWeaviateSchemaThingsPropertiesDeleteForbidden() *WeaviateSchemaThingsPropertiesDeleteForbidden {

	return &WeaviateSchemaThingsPropertiesDeleteForbidden{}
}

// WriteResponse to the client
func (o *WeaviateSchemaThingsPropertiesDeleteForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(403)
}
