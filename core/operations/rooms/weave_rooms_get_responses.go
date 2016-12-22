package rooms




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/core/models"
)

/*WeaveRoomsGetOK Successful response

swagger:response weaveRoomsGetOK
*/
type WeaveRoomsGetOK struct {

	// In: body
	Payload *models.Room `json:"body,omitempty"`
}

// NewWeaveRoomsGetOK creates WeaveRoomsGetOK with default headers values
func NewWeaveRoomsGetOK() *WeaveRoomsGetOK {
	return &WeaveRoomsGetOK{}
}

// WithPayload adds the payload to the weave rooms get o k response
func (o *WeaveRoomsGetOK) WithPayload(payload *models.Room) *WeaveRoomsGetOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave rooms get o k response
func (o *WeaveRoomsGetOK) SetPayload(payload *models.Room) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaveRoomsGetOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
