package devices




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/core/models"
)

/*WeaveDevicesUpdateOK Successful response

swagger:response weaveDevicesUpdateOK
*/
type WeaveDevicesUpdateOK struct {

	// In: body
	Payload *models.Device `json:"body,omitempty"`
}

// NewWeaveDevicesUpdateOK creates WeaveDevicesUpdateOK with default headers values
func NewWeaveDevicesUpdateOK() *WeaveDevicesUpdateOK {
	return &WeaveDevicesUpdateOK{}
}

// WithPayload adds the payload to the weave devices update o k response
func (o *WeaveDevicesUpdateOK) WithPayload(payload *models.Device) *WeaveDevicesUpdateOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave devices update o k response
func (o *WeaveDevicesUpdateOK) SetPayload(payload *models.Device) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaveDevicesUpdateOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
