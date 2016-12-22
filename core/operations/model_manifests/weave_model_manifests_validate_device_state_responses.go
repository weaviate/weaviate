package model_manifests




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/core/models"
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
