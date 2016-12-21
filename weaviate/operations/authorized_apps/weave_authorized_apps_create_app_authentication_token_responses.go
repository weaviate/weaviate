package authorized_apps




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/weaviate/models"
)

/*WeaveAuthorizedAppsCreateAppAuthenticationTokenOK Successful response

swagger:response weaveAuthorizedAppsCreateAppAuthenticationTokenOK
*/
type WeaveAuthorizedAppsCreateAppAuthenticationTokenOK struct {

	// In: body
	Payload *models.AuthorizedAppsCreateAppAuthenticationTokenResponse `json:"body,omitempty"`
}

// NewWeaveAuthorizedAppsCreateAppAuthenticationTokenOK creates WeaveAuthorizedAppsCreateAppAuthenticationTokenOK with default headers values
func NewWeaveAuthorizedAppsCreateAppAuthenticationTokenOK() *WeaveAuthorizedAppsCreateAppAuthenticationTokenOK {
	return &WeaveAuthorizedAppsCreateAppAuthenticationTokenOK{}
}

// WithPayload adds the payload to the weave authorized apps create app authentication token o k response
func (o *WeaveAuthorizedAppsCreateAppAuthenticationTokenOK) WithPayload(payload *models.AuthorizedAppsCreateAppAuthenticationTokenResponse) *WeaveAuthorizedAppsCreateAppAuthenticationTokenOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave authorized apps create app authentication token o k response
func (o *WeaveAuthorizedAppsCreateAppAuthenticationTokenOK) SetPayload(payload *models.AuthorizedAppsCreateAppAuthenticationTokenResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaveAuthorizedAppsCreateAppAuthenticationTokenOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
