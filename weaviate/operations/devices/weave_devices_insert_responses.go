package devices




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/weaviate/models"
)

/*WeaveDevicesInsertOK Successful response

swagger:response weaveDevicesInsertOK
*/
type WeaveDevicesInsertOK struct {

	// In: body
	Payload *models.Device `json:"body,omitempty"`
}

// NewWeaveDevicesInsertOK creates WeaveDevicesInsertOK with default headers values
func NewWeaveDevicesInsertOK() *WeaveDevicesInsertOK {
	return &WeaveDevicesInsertOK{}
}

// WithPayload adds the payload to the weave devices insert o k response
func (o *WeaveDevicesInsertOK) WithPayload(payload *models.Device) *WeaveDevicesInsertOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave devices insert o k response
func (o *WeaveDevicesInsertOK) SetPayload(payload *models.Device) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaveDevicesInsertOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
