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

/*WeaveDevicesCreateLocalAuthTokensOK Successful response

swagger:response weaveDevicesCreateLocalAuthTokensOK
*/
type WeaveDevicesCreateLocalAuthTokensOK struct {

	// In: body
	Payload *models.DevicesCreateLocalAuthTokensResponse `json:"body,omitempty"`
}

// NewWeaveDevicesCreateLocalAuthTokensOK creates WeaveDevicesCreateLocalAuthTokensOK with default headers values
func NewWeaveDevicesCreateLocalAuthTokensOK() *WeaveDevicesCreateLocalAuthTokensOK {
	return &WeaveDevicesCreateLocalAuthTokensOK{}
}

// WithPayload adds the payload to the weave devices create local auth tokens o k response
func (o *WeaveDevicesCreateLocalAuthTokensOK) WithPayload(payload *models.DevicesCreateLocalAuthTokensResponse) *WeaveDevicesCreateLocalAuthTokensOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave devices create local auth tokens o k response
func (o *WeaveDevicesCreateLocalAuthTokensOK) SetPayload(payload *models.DevicesCreateLocalAuthTokensResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaveDevicesCreateLocalAuthTokensOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
