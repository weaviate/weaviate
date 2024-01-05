//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package crossref

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/models"
)

const (
	_LocalHost = "localhost"
	_Schema    = "weaviate"
)

// Ref is an abstraction of the cross-refs which are specified in a URI format
// in the API. When this type is used it is safe to assume that a Ref is
// semantically valid. This guarantee would not be possible on the URI format,
// as the URI can be well-formed, but not contain the data we expect in it.
// Do not use directly, such as crossref.Ref{}, as you won't have any
// guarantees in this case. Always use one of the parsing options or New()
type Ref struct {
	Local    bool        `json:"local"`
	PeerName string      `json:"peerName"`
	TargetID strfmt.UUID `json:"targetID"`
	Class    string      `json:"className"`
}

// Parse is a safe way to generate a Ref, as it will error if any of the input
// parameters are not as expected.
func Parse(uriString string) (*Ref, error) {
	uri, err := url.Parse(uriString)
	if err != nil || uri.Path == "" {
		return nil, fmt.Errorf("invalid cref URI: %s", err)
	}

	segments := strings.Split(uri.Path, "/")
	class, id, idx := "", "", 1
	switch len(segments) {
	case 3:
		class = segments[1]
		id = segments[2]
		idx = 2
	case 2:
		id = segments[1]
	default:
		return nil, fmt.Errorf(
			"invalid cref URI: path must be of format '<class>/<uuid>', but got '%s'", uri.Path)
	}
	if ok := strfmt.IsUUID(id); !ok {
		return nil, fmt.Errorf("invalid cref URI: %dnd path segment must be uuid, but got '%s'",
			idx, id)
	}

	return &Ref{
		Local:    uri.Host == _LocalHost,
		PeerName: uri.Host,
		TargetID: strfmt.UUID(id),
		Class:    class,
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
func New(peerName string, class string, target strfmt.UUID) *Ref {
	return &Ref{
		Local:    peerName == _LocalHost,
		PeerName: peerName,
		TargetID: target,
		Class:    class,
	}
}

func NewLocalhost(class string, target strfmt.UUID) *Ref {
	return New(_LocalHost, class, target)
}

func (r *Ref) String() string {
	path := fmt.Sprintf("%s/%s", r.Class, r.TargetID)
	if r.Class == "" {
		path = fmt.Sprintf("/%s", r.TargetID)
	}
	uri := url.URL{
		Host:   r.PeerName,
		Scheme: _Schema,
		Path:   path,
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
