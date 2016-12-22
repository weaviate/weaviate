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

	"github.com/weaviate/weaviate/core/models"
)

/*WeaveModelManifestsValidateCommandDefsOK Successful response

swagger:response weaveModelManifestsValidateCommandDefsOK
*/
type WeaveModelManifestsValidateCommandDefsOK struct {

	// In: body
	Payload *models.ModelManifestsValidateCommandDefsResponse `json:"body,omitempty"`
}

// NewWeaveModelManifestsValidateCommandDefsOK creates WeaveModelManifestsValidateCommandDefsOK with default headers values
func NewWeaveModelManifestsValidateCommandDefsOK() *WeaveModelManifestsValidateCommandDefsOK {
	return &WeaveModelManifestsValidateCommandDefsOK{}
}

// WithPayload adds the payload to the weave model manifests validate command defs o k response
func (o *WeaveModelManifestsValidateCommandDefsOK) WithPayload(payload *models.ModelManifestsValidateCommandDefsResponse) *WeaveModelManifestsValidateCommandDefsOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave model manifests validate command defs o k response
func (o *WeaveModelManifestsValidateCommandDefsOK) SetPayload(payload *models.ModelManifestsValidateCommandDefsResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaveModelManifestsValidateCommandDefsOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
