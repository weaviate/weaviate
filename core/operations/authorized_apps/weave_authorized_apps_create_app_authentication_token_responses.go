/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 Weaviate. All rights reserved.
 * LICENSE: https://github.com/weaviate/weaviate/blob/master/LICENSE
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * See package.json for author and maintainer info
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */
 package authorized_apps




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/core/models"
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
