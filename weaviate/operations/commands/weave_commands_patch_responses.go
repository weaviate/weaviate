package commands




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/weaviate/models"
)

/*WeaveCommandsPatchOK Successful response

swagger:response weaveCommandsPatchOK
*/
type WeaveCommandsPatchOK struct {

	// In: body
	Payload *models.Command `json:"body,omitempty"`
}

// NewWeaveCommandsPatchOK creates WeaveCommandsPatchOK with default headers values
func NewWeaveCommandsPatchOK() *WeaveCommandsPatchOK {
	return &WeaveCommandsPatchOK{}
}

// WithPayload adds the payload to the weave commands patch o k response
func (o *WeaveCommandsPatchOK) WithPayload(payload *models.Command) *WeaveCommandsPatchOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave commands patch o k response
func (o *WeaveCommandsPatchOK) SetPayload(payload *models.Command) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaveCommandsPatchOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
