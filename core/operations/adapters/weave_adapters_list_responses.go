package adapters




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/core/models"
)

/*WeaveAdaptersListOK Successful response

swagger:response weaveAdaptersListOK
*/
type WeaveAdaptersListOK struct {

	// In: body
	Payload *models.AdaptersListResponse `json:"body,omitempty"`
}

// NewWeaveAdaptersListOK creates WeaveAdaptersListOK with default headers values
func NewWeaveAdaptersListOK() *WeaveAdaptersListOK {
	return &WeaveAdaptersListOK{}
}

// WithPayload adds the payload to the weave adapters list o k response
func (o *WeaveAdaptersListOK) WithPayload(payload *models.AdaptersListResponse) *WeaveAdaptersListOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave adapters list o k response
func (o *WeaveAdaptersListOK) SetPayload(payload *models.AdaptersListResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaveAdaptersListOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
