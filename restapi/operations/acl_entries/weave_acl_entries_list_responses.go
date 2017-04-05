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
 package acl_entries




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/models"
)

/*WeaveACLEntriesListOK Successful response

swagger:response weaveAclEntriesListOK
*/
type WeaveACLEntriesListOK struct {

	// In: body
	Payload *models.ACLEntriesListResponse `json:"body,omitempty"`
}

// NewWeaveACLEntriesListOK creates WeaveACLEntriesListOK with default headers values
func NewWeaveACLEntriesListOK() *WeaveACLEntriesListOK {
	return &WeaveACLEntriesListOK{}
}

// WithPayload adds the payload to the weave Acl entries list o k response
func (o *WeaveACLEntriesListOK) WithPayload(payload *models.ACLEntriesListResponse) *WeaveACLEntriesListOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weave Acl entries list o k response
func (o *WeaveACLEntriesListOK) SetPayload(payload *models.ACLEntriesListResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaveACLEntriesListOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		if err := producer.Produce(rw, o.Payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
