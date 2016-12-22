package commands




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/core/models"
)

/*WeaveCommandsInsertOK Successful response

swagger:response weaveCommandsInsertOK
*/
type WeaveCommandsInsertOK struct {

	// In: body
	Payload *models.Command `json:"body,omitempty"`
}

// NewWeaveCommandsInsertOK creates WeaveCommandsInsertOK with default headers values
func NewWeaveCommandsInsertOK() *WeaveCommandsInsertOK {
	return &WeaveCommandsInsertOK{}
}

// WithPayload adds the payload to the weave commands insert o k response
func (o *WeaveCommandsInsertOK) WithPayload(payload *models.Command) *WeaveCommandsInsertOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave commands insert o k response
func (o *WeaveCommandsInsertOK) SetPayload(payload *models.Command) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaveCommandsInsertOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
