package model_manifests




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/core/models"
)

/*WeaveModelManifestsListOK Successful response

swagger:response weaveModelManifestsListOK
*/
type WeaveModelManifestsListOK struct {

	// In: body
	Payload *models.ModelManifestsListResponse `json:"body,omitempty"`
}

// NewWeaveModelManifestsListOK creates WeaveModelManifestsListOK with default headers values
func NewWeaveModelManifestsListOK() *WeaveModelManifestsListOK {
	return &WeaveModelManifestsListOK{}
}

// WithPayload adds the payload to the weave model manifests list o k response
func (o *WeaveModelManifestsListOK) WithPayload(payload *models.ModelManifestsListResponse) *WeaveModelManifestsListOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave model manifests list o k response
func (o *WeaveModelManifestsListOK) SetPayload(payload *models.ModelManifestsListResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaveModelManifestsListOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
