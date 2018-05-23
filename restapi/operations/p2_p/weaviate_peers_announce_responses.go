/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */

package p2_p

import (
	"net/http"

	"github.com/go-openapi/runtime"
)

// WeaviatePeersAnnounceOKCode is the HTTP code returned for type WeaviatePeersAnnounceOK
const WeaviatePeersAnnounceOKCode int = 200

/*WeaviatePeersAnnounceOK Successfully registred the peer to the network.

swagger:response weaviatePeersAnnounceOK
*/
type WeaviatePeersAnnounceOK struct {
}

// NewWeaviatePeersAnnounceOK creates WeaviatePeersAnnounceOK with default headers values
func NewWeaviatePeersAnnounceOK() *WeaviatePeersAnnounceOK {
	return &WeaviatePeersAnnounceOK{}
}

// WriteResponse to the client
func (o *WeaviatePeersAnnounceOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
}

// WeaviatePeersAnnounceForbiddenCode is the HTTP code returned for type WeaviatePeersAnnounceForbidden
const WeaviatePeersAnnounceForbiddenCode int = 403

/*WeaviatePeersAnnounceForbidden You are not allowed on the network.

swagger:response weaviatePeersAnnounceForbidden
*/
type WeaviatePeersAnnounceForbidden struct {
}

// NewWeaviatePeersAnnounceForbidden creates WeaviatePeersAnnounceForbidden with default headers values
func NewWeaviatePeersAnnounceForbidden() *WeaviatePeersAnnounceForbidden {
	return &WeaviatePeersAnnounceForbidden{}
}

// WriteResponse to the client
func (o *WeaviatePeersAnnounceForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviatePeersAnnounceNotImplementedCode is the HTTP code returned for type WeaviatePeersAnnounceNotImplemented
const WeaviatePeersAnnounceNotImplementedCode int = 501

/*WeaviatePeersAnnounceNotImplemented Not (yet) implemented.

swagger:response weaviatePeersAnnounceNotImplemented
*/
type WeaviatePeersAnnounceNotImplemented struct {
}

// NewWeaviatePeersAnnounceNotImplemented creates WeaviatePeersAnnounceNotImplemented with default headers values
func NewWeaviatePeersAnnounceNotImplemented() *WeaviatePeersAnnounceNotImplemented {
	return &WeaviatePeersAnnounceNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviatePeersAnnounceNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
