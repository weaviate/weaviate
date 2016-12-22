package events




import (
	"net/http"

	"github.com/go-openapi/runtime"
)

/*WeaveEventsRecordDeviceEventsOK Successful response

swagger:response weaveEventsRecordDeviceEventsOK
*/
type WeaveEventsRecordDeviceEventsOK struct {
}

// NewWeaveEventsRecordDeviceEventsOK creates WeaveEventsRecordDeviceEventsOK with default headers values
func NewWeaveEventsRecordDeviceEventsOK() *WeaveEventsRecordDeviceEventsOK {
	return &WeaveEventsRecordDeviceEventsOK{}
}

// WriteResponse to the client
func (o *WeaveEventsRecordDeviceEventsOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
}
