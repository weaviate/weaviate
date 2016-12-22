package commands




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/core/models"
)

/*WeaveCommandsListOK Successful response

swagger:response weaveCommandsListOK
*/
type WeaveCommandsListOK struct {

	// In: body
	Payload *models.CommandsListResponse `json:"body,omitempty"`
}

// NewWeaveCommandsListOK creates WeaveCommandsListOK with default headers values
func NewWeaveCommandsListOK() *WeaveCommandsListOK {
	return &WeaveCommandsListOK{}
}

// WithPayload adds the payload to the weave commands list o k response
func (o *WeaveCommandsListOK) WithPayload(payload *models.CommandsListResponse) *WeaveCommandsListOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave commands list o k response
func (o *WeaveCommandsListOK) SetPayload(payload *models.CommandsListResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaveCommandsListOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
