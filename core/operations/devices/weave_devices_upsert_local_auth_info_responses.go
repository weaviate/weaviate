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
 package devices




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/core/models"
)

/*WeaveDevicesUpsertLocalAuthInfoOK Successful response

swagger:response weaveDevicesUpsertLocalAuthInfoOK
*/
type WeaveDevicesUpsertLocalAuthInfoOK struct {

	// In: body
	Payload *models.DevicesUpsertLocalAuthInfoResponse `json:"body,omitempty"`
}

// NewWeaveDevicesUpsertLocalAuthInfoOK creates WeaveDevicesUpsertLocalAuthInfoOK with default headers values
func NewWeaveDevicesUpsertLocalAuthInfoOK() *WeaveDevicesUpsertLocalAuthInfoOK {
	return &WeaveDevicesUpsertLocalAuthInfoOK{}
}

// WithPayload adds the payload to the weave devices upsert local auth info o k response
func (o *WeaveDevicesUpsertLocalAuthInfoOK) WithPayload(payload *models.DevicesUpsertLocalAuthInfoResponse) *WeaveDevicesUpsertLocalAuthInfoOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave devices upsert local auth info o k response
func (o *WeaveDevicesUpsertLocalAuthInfoOK) SetPayload(payload *models.DevicesUpsertLocalAuthInfoResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaveDevicesUpsertLocalAuthInfoOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
