package commands




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/weaviate/models"
)

/*WeaveCommandsCancelOK Successful response

swagger:response weaveCommandsCancelOK
*/
type WeaveCommandsCancelOK struct {

	// In: body
	Payload *models.Command `json:"body,omitempty"`
}

// NewWeaveCommandsCancelOK creates WeaveCommandsCancelOK with default headers values
func NewWeaveCommandsCancelOK() *WeaveCommandsCancelOK {
	return &WeaveCommandsCancelOK{}
}

// WithPayload adds the payload to the weave commands cancel o k response
func (o *WeaveCommandsCancelOK) WithPayload(payload *models.Command) *WeaveCommandsCancelOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave commands cancel o k response
func (o *WeaveCommandsCancelOK) SetPayload(payload *models.Command) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaveCommandsCancelOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
