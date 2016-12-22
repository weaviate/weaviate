package devices




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/core/models"
)

/*WeaveDevicesListOK Successful response

swagger:response weaveDevicesListOK
*/
type WeaveDevicesListOK struct {

	// In: body
	Payload *models.DevicesListResponse `json:"body,omitempty"`
}

// NewWeaveDevicesListOK creates WeaveDevicesListOK with default headers values
func NewWeaveDevicesListOK() *WeaveDevicesListOK {
	return &WeaveDevicesListOK{}
}

// WithPayload adds the payload to the weave devices list o k response
func (o *WeaveDevicesListOK) WithPayload(payload *models.DevicesListResponse) *WeaveDevicesListOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave devices list o k response
func (o *WeaveDevicesListOK) SetPayload(payload *models.DevicesListResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaveDevicesListOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
