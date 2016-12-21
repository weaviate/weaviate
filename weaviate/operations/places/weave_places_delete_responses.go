package places




import (
	"net/http"

	"github.com/go-openapi/runtime"
)

/*WeavePlacesDeleteOK Successful response

swagger:response weavePlacesDeleteOK
*/
type WeavePlacesDeleteOK struct {
}

// NewWeavePlacesDeleteOK creates WeavePlacesDeleteOK with default headers values
func NewWeavePlacesDeleteOK() *WeavePlacesDeleteOK {
	return &WeavePlacesDeleteOK{}
}

// WriteResponse to the client
func (o *WeavePlacesDeleteOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
}
