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
