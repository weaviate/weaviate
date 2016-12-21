package model_manifests


// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/weaviate/models"
)

/*WeaveModelManifestsGetOK Successful response

swagger:response weaveModelManifestsGetOK
*/
type WeaveModelManifestsGetOK struct {

	// In: body
	Payload *models.ModelManifest `json:"body,omitempty"`
}

// NewWeaveModelManifestsGetOK creates WeaveModelManifestsGetOK with default headers values
func NewWeaveModelManifestsGetOK() *WeaveModelManifestsGetOK {
	return &WeaveModelManifestsGetOK{}
}

// WithPayload adds the payload to the weave model manifests get o k response
func (o *WeaveModelManifestsGetOK) WithPayload(payload *models.ModelManifest) *WeaveModelManifestsGetOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave model manifests get o k response
func (o *WeaveModelManifestsGetOK) SetPayload(payload *models.ModelManifest) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaveModelManifestsGetOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
