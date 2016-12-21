package devices




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/weaviate/models"
)

/*WeaveDevicesAddtoplaceOK Successful response

swagger:response weaveDevicesAddtoplaceOK
*/
type WeaveDevicesAddtoplaceOK struct {

	// In: body
	Payload *models.Device `json:"body,omitempty"`
}

// NewWeaveDevicesAddtoplaceOK creates WeaveDevicesAddtoplaceOK with default headers values
func NewWeaveDevicesAddtoplaceOK() *WeaveDevicesAddtoplaceOK {
	return &WeaveDevicesAddtoplaceOK{}
}

// WithPayload adds the payload to the weave devices addtoplace o k response
func (o *WeaveDevicesAddtoplaceOK) WithPayload(payload *models.Device) *WeaveDevicesAddtoplaceOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave devices addtoplace o k response
func (o *WeaveDevicesAddtoplaceOK) SetPayload(payload *models.Device) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaveDevicesAddtoplaceOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
