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
 * See package.json for author and maintainer info
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */
 package rooms




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/core/models"
)

/*WeaveRoomsModifyOK Successful response

swagger:response weaveRoomsModifyOK
*/
type WeaveRoomsModifyOK struct {

	// In: body
	Payload *models.Room `json:"body,omitempty"`
}

// NewWeaveRoomsModifyOK creates WeaveRoomsModifyOK with default headers values
func NewWeaveRoomsModifyOK() *WeaveRoomsModifyOK {
	return &WeaveRoomsModifyOK{}
}

// WithPayload adds the payload to the weave rooms modify o k response
func (o *WeaveRoomsModifyOK) WithPayload(payload *models.Room) *WeaveRoomsModifyOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave rooms modify o k response
func (o *WeaveRoomsModifyOK) SetPayload(payload *models.Room) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaveRoomsModifyOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
