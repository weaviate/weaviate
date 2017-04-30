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

// WeaviateCommandsInsertCreatedCode is the HTTP code returned for type WeaviateCommandsInsertCreated
const WeaviateCommandsInsertCreatedCode int = 201

/*WeaviateCommandsInsertCreated Successful created.

swagger:response weaviateCommandsInsertCreated
*/
type WeaviateCommandsInsertCreated struct {

	/*
	  In: Body
	*/
	Payload *models.Command `json:"body,omitempty"`
}

// NewWeaviateCommandsInsertCreated creates WeaviateCommandsInsertCreated with default headers values
func NewWeaviateCommandsInsertCreated() *WeaviateCommandsInsertCreated {
	return &WeaviateCommandsInsertCreated{}
}

// WithPayload adds the payload to the weaviate commands insert created response
func (o *WeaviateCommandsInsertCreated) WithPayload(payload *models.Command) *WeaviateCommandsInsertCreated {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate commands insert created response
func (o *WeaviateCommandsInsertCreated) SetPayload(payload *models.Command) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateCommandsInsertCreated) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(201)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateCommandsInsertNotImplementedCode is the HTTP code returned for type WeaviateCommandsInsertNotImplemented
const WeaviateCommandsInsertNotImplementedCode int = 501

/*WeaviateCommandsInsertNotImplemented Not (yet) implemented.

swagger:response weaviateCommandsInsertNotImplemented
*/
type WeaviateCommandsInsertNotImplemented struct {
}

// NewWeaviateCommandsInsertNotImplemented creates WeaviateCommandsInsertNotImplemented with default headers values
func NewWeaviateCommandsInsertNotImplemented() *WeaviateCommandsInsertNotImplemented {
	return &WeaviateCommandsInsertNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateCommandsInsertNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
