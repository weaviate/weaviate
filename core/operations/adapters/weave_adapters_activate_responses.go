package adapters




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/core/models"
)

/*WeaveAdaptersActivateOK Successful response

swagger:response weaveAdaptersActivateOK
*/
type WeaveAdaptersActivateOK struct {

	// In: body
	Payload *models.AdaptersActivateResponse `json:"body,omitempty"`
}

// NewWeaveAdaptersActivateOK creates WeaveAdaptersActivateOK with default headers values
func NewWeaveAdaptersActivateOK() *WeaveAdaptersActivateOK {
	return &WeaveAdaptersActivateOK{}
}

// WithPayload adds the payload to the weave adapters activate o k response
func (o *WeaveAdaptersActivateOK) WithPayload(payload *models.AdaptersActivateResponse) *WeaveAdaptersActivateOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave adapters activate o k response
func (o *WeaveAdaptersActivateOK) SetPayload(payload *models.AdaptersActivateResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaveAdaptersActivateOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
