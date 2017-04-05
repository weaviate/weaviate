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

	"github.com/weaviate/weaviate/models"
)

/*WeaveDevicesGetOK Successful response

swagger:response weaveDevicesGetOK
*/
type WeaveDevicesGetOK struct {

	// In: body
	Payload *models.Device `json:"body,omitempty"`
}

// NewWeaveDevicesGetOK creates WeaveDevicesGetOK with default headers values
func NewWeaveDevicesGetOK() *WeaveDevicesGetOK {
	return &WeaveDevicesGetOK{}
}

// WithPayload adds the payload to the weave devices get o k response
func (o *WeaveDevicesGetOK) WithPayload(payload *models.Device) *WeaveDevicesGetOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave devices get o k response
func (o *WeaveDevicesGetOK) SetPayload(payload *models.Device) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaveDevicesGetOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
