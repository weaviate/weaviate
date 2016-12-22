package adapters




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/core/models"
)

/*WeaveAdaptersGetOK Successful response

swagger:response weaveAdaptersGetOK
*/
type WeaveAdaptersGetOK struct {

	// In: body
	Payload *models.Adapter `json:"body,omitempty"`
}

// NewWeaveAdaptersGetOK creates WeaveAdaptersGetOK with default headers values
func NewWeaveAdaptersGetOK() *WeaveAdaptersGetOK {
	return &WeaveAdaptersGetOK{}
}

// WithPayload adds the payload to the weave adapters get o k response
func (o *WeaveAdaptersGetOK) WithPayload(payload *models.Adapter) *WeaveAdaptersGetOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave adapters get o k response
func (o *WeaveAdaptersGetOK) SetPayload(payload *models.Adapter) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaveAdaptersGetOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
