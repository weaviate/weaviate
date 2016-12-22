package devices




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/core/models"
)

/*WeaveDevicesPatchStateOK Successful response

swagger:response weaveDevicesPatchStateOK
*/
type WeaveDevicesPatchStateOK struct {

	// In: body
	Payload *models.DevicesPatchStateResponse `json:"body,omitempty"`
}

// NewWeaveDevicesPatchStateOK creates WeaveDevicesPatchStateOK with default headers values
func NewWeaveDevicesPatchStateOK() *WeaveDevicesPatchStateOK {
	return &WeaveDevicesPatchStateOK{}
}

// WithPayload adds the payload to the weave devices patch state o k response
func (o *WeaveDevicesPatchStateOK) WithPayload(payload *models.DevicesPatchStateResponse) *WeaveDevicesPatchStateOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave devices patch state o k response
func (o *WeaveDevicesPatchStateOK) SetPayload(payload *models.DevicesPatchStateResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaveDevicesPatchStateOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
