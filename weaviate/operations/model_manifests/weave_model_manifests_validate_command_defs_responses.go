package model_manifests


// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/weaviate/models"
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
