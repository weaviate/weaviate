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
 package model_manifests




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/models"
)

/*WeaveModelManifestsValidateDeviceStateOK Successful response

swagger:response weaveModelManifestsValidateDeviceStateOK
*/
type WeaveModelManifestsValidateDeviceStateOK struct {

	// In: body
	Payload *models.ModelManifestsValidateDeviceStateResponse `json:"body,omitempty"`
}

// NewWeaveModelManifestsValidateDeviceStateOK creates WeaveModelManifestsValidateDeviceStateOK with default headers values
func NewWeaveModelManifestsValidateDeviceStateOK() *WeaveModelManifestsValidateDeviceStateOK {
	return &WeaveModelManifestsValidateDeviceStateOK{}
}

// WithPayload adds the payload to the weave model manifests validate device state o k response
func (o *WeaveModelManifestsValidateDeviceStateOK) WithPayload(payload *models.ModelManifestsValidateDeviceStateResponse) *WeaveModelManifestsValidateDeviceStateOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave model manifests validate device state o k response
func (o *WeaveModelManifestsValidateDeviceStateOK) SetPayload(payload *models.ModelManifestsValidateDeviceStateResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaveModelManifestsValidateDeviceStateOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
