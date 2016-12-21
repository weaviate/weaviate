package devices




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/weaviate/models"
)

/*WeaveDevicesPatchOK Successful response

swagger:response weaveDevicesPatchOK
*/
type WeaveDevicesPatchOK struct {

	// In: body
	Payload *models.Device `json:"body,omitempty"`
}

// NewWeaveDevicesPatchOK creates WeaveDevicesPatchOK with default headers values
func NewWeaveDevicesPatchOK() *WeaveDevicesPatchOK {
	return &WeaveDevicesPatchOK{}
}

// WithPayload adds the payload to the weave devices patch o k response
func (o *WeaveDevicesPatchOK) WithPayload(payload *models.Device) *WeaveDevicesPatchOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave devices patch o k response
func (o *WeaveDevicesPatchOK) SetPayload(payload *models.Device) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaveDevicesPatchOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
