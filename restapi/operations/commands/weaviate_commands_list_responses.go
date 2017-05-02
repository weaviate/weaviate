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
 package commands




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/models"
)

// WeaviateCommandsListOKCode is the HTTP code returned for type WeaviateCommandsListOK
const WeaviateCommandsListOKCode int = 200

/*WeaviateCommandsListOK Successful response.

swagger:response weaviateCommandsListOK
*/
type WeaviateCommandsListOK struct {

	/*
	  In: Body
	*/
	Payload *models.CommandsListResponse `json:"body,omitempty"`
}

// NewWeaviateCommandsListOK creates WeaviateCommandsListOK with default headers values
func NewWeaviateCommandsListOK() *WeaviateCommandsListOK {
	return &WeaviateCommandsListOK{}
}

// WithPayload adds the payload to the weaviate commands list o k response
func (o *WeaviateCommandsListOK) WithPayload(payload *models.CommandsListResponse) *WeaviateCommandsListOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate commands list o k response
func (o *WeaviateCommandsListOK) SetPayload(payload *models.CommandsListResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateCommandsListOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateCommandsListNotFoundCode is the HTTP code returned for type WeaviateCommandsListNotFound
const WeaviateCommandsListNotFoundCode int = 404

/*WeaviateCommandsListNotFound Successful query result but no resource was found

swagger:response weaviateCommandsListNotFound
*/
type WeaviateCommandsListNotFound struct {
}

// NewWeaviateCommandsListNotFound creates WeaviateCommandsListNotFound with default headers values
func NewWeaviateCommandsListNotFound() *WeaviateCommandsListNotFound {
	return &WeaviateCommandsListNotFound{}
}

// WriteResponse to the client
func (o *WeaviateCommandsListNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateCommandsListNotImplementedCode is the HTTP code returned for type WeaviateCommandsListNotImplemented
const WeaviateCommandsListNotImplementedCode int = 501

/*WeaviateCommandsListNotImplemented Not (yet) implemented.

swagger:response weaviateCommandsListNotImplemented
*/
type WeaviateCommandsListNotImplemented struct {
}

// NewWeaviateCommandsListNotImplemented creates WeaviateCommandsListNotImplemented with default headers values
func NewWeaviateCommandsListNotImplemented() *WeaviateCommandsListNotImplemented {
	return &WeaviateCommandsListNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateCommandsListNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
