package places


// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/weaviate/models"
)

/*WeavePlacesListSuggestionsOK Successful response

swagger:response weavePlacesListSuggestionsOK
*/
type WeavePlacesListSuggestionsOK struct {

	// In: body
	Payload *models.PlacesListSuggestionsResponse `json:"body,omitempty"`
}

// NewWeavePlacesListSuggestionsOK creates WeavePlacesListSuggestionsOK with default headers values
func NewWeavePlacesListSuggestionsOK() *WeavePlacesListSuggestionsOK {
	return &WeavePlacesListSuggestionsOK{}
}

// WithPayload adds the payload to the weave places list suggestions o k response
func (o *WeavePlacesListSuggestionsOK) WithPayload(payload *models.PlacesListSuggestionsResponse) *WeavePlacesListSuggestionsOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave places list suggestions o k response
func (o *WeavePlacesListSuggestionsOK) SetPayload(payload *models.PlacesListSuggestionsResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeavePlacesListSuggestionsOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
