package devices




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/weaviate/models"
)

/*WeaveDevicesUpsertLocalAuthInfoOK Successful response

swagger:response weaveDevicesUpsertLocalAuthInfoOK
*/
type WeaveDevicesUpsertLocalAuthInfoOK struct {

	// In: body
	Payload *models.DevicesUpsertLocalAuthInfoResponse `json:"body,omitempty"`
}

// NewWeaveDevicesUpsertLocalAuthInfoOK creates WeaveDevicesUpsertLocalAuthInfoOK with default headers values
func NewWeaveDevicesUpsertLocalAuthInfoOK() *WeaveDevicesUpsertLocalAuthInfoOK {
	return &WeaveDevicesUpsertLocalAuthInfoOK{}
}

// WithPayload adds the payload to the weave devices upsert local auth info o k response
func (o *WeaveDevicesUpsertLocalAuthInfoOK) WithPayload(payload *models.DevicesUpsertLocalAuthInfoResponse) *WeaveDevicesUpsertLocalAuthInfoOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave devices upsert local auth info o k response
func (o *WeaveDevicesUpsertLocalAuthInfoOK) SetPayload(payload *models.DevicesUpsertLocalAuthInfoResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaveDevicesUpsertLocalAuthInfoOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
