//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package crossref

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
)

// Ref is an abstraction of the cross-refs which are specified in a URI format
// in the API. When this type is used it is safe to assume that a Ref is
// semantically valid. This guarantuee would not be possible on the URI format,
// as the URI can be well-formed, but not contain the data we expect in it.
// Do not use directly, such as crossref.Ref{}, as you won't have any
// guarantees in this case. Always use one of the parsing options or New()
type Ref struct {
	Local    bool        `json:"local"`
	PeerName string      `json:"peerName"`
	TargetID strfmt.UUID `json:"targetID"`
}

// Parse is a safe way to generate a Ref, as it will error if any of the input
// parameters are not as expected.
func Parse(uriString string) (*Ref, error) {
	uri, err := url.Parse(uriString)
	if err != nil {
		return nil, fmt.Errorf("invalid cref URI: %s", err)
	}

	pathSegments := strings.Split(uri.Path, "/")
	if len(pathSegments) != 2 {
		return nil, fmt.Errorf(
			"invalid cref URI: path must be of format '/<uuid>', but got '%s'",
			uri.Path)
	}

	if ok := strfmt.IsUUID(pathSegments[1]); !ok {
		return nil, fmt.Errorf("invalid cref URI: 2nd path segment must be uuid, but got '%s'",
			pathSegments[1])
	}

	return &Ref{
		Local:    (uri.Host == "localhost"),
		PeerName: uri.Host,
		TargetID: strfmt.UUID(pathSegments[1]),
	}, nil
}

// ParseSingleRef is a safe way to generate a Ref from a models.SingleRef, a
// helper construct that represents the API structure. It will error if any of
// the input parameters are not as expected.
func ParseSingleRef(singleRef *models.SingleRef) (*Ref, error) {
	return Parse(string(singleRef.Beacon))
}

// New is a safe way to generate a Reference, as all required arguments must be
// set in the constructor fn
func New(peerName string, target strfmt.UUID) *Ref {
	return &Ref{
		Local:    (peerName == "localhost"),
		PeerName: peerName,
		TargetID: target,
	}
}

func (r *Ref) String() string {
	uri := url.URL{
		Host:   r.PeerName,
		Scheme: "weaviate",
		Path:   fmt.Sprintf("/%s", r.TargetID),
	}

	return uri.String()
}

// SingleRef converts the parsed Ref back into the API helper construct
// containing a stringified representation (URI format) of the Ref
func (r *Ref) SingleRef() *models.SingleRef {
	return &models.SingleRef{
		Beacon: strfmt.URI(r.String()),
	}
}
