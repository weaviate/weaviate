package devices




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/core/models"
)

/*WeaveDevicesSetroomOK Successful response

swagger:response weaveDevicesSetroomOK
*/
type WeaveDevicesSetroomOK struct {

	// In: body
	Payload *models.Device `json:"body,omitempty"`
}

// NewWeaveDevicesSetroomOK creates WeaveDevicesSetroomOK with default headers values
func NewWeaveDevicesSetroomOK() *WeaveDevicesSetroomOK {
	return &WeaveDevicesSetroomOK{}
}

// WithPayload adds the payload to the weave devices setroom o k response
func (o *WeaveDevicesSetroomOK) WithPayload(payload *models.Device) *WeaveDevicesSetroomOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave devices setroom o k response
func (o *WeaveDevicesSetroomOK) SetPayload(payload *models.Device) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaveDevicesSetroomOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
