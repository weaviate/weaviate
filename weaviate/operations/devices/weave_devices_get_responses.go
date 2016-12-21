package devices




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/weaviate/models"
)

/*WeaveDevicesGetOK Successful response

swagger:response weaveDevicesGetOK
*/
type WeaveDevicesGetOK struct {

	// In: body
	Payload *models.Device `json:"body,omitempty"`
}

// NewWeaveDevicesGetOK creates WeaveDevicesGetOK with default headers values
func NewWeaveDevicesGetOK() *WeaveDevicesGetOK {
	return &WeaveDevicesGetOK{}
}

// WithPayload adds the payload to the weave devices get o k response
func (o *WeaveDevicesGetOK) WithPayload(payload *models.Device) *WeaveDevicesGetOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave devices get o k response
func (o *WeaveDevicesGetOK) SetPayload(payload *models.Device) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaveDevicesGetOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
