package subscriptions


// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"
)

/*WeaveSubscriptionsDeleteOK Successful response

swagger:response weaveSubscriptionsDeleteOK
*/
type WeaveSubscriptionsDeleteOK struct {
}

// NewWeaveSubscriptionsDeleteOK creates WeaveSubscriptionsDeleteOK with default headers values
func NewWeaveSubscriptionsDeleteOK() *WeaveSubscriptionsDeleteOK {
	return &WeaveSubscriptionsDeleteOK{}
}

// WriteResponse to the client
func (o *WeaveSubscriptionsDeleteOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
}
