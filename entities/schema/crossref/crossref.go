/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
 * LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
 * CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */
package crossref

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
)

// Ref is an abstraction of the cross-refs which are specified in a URI format
// in the API. When this type is used it is safe to assume that a Ref is
// semantically valid. This guarantuee would not be possible on the URI format,
// as the URI can be well-formed, but not contain the data we expect in it.
// Do not use directly, such as crossref.Ref{}, as you won't have any
// guarantees in this case. Always use one of the parsing options or New()
type Ref struct {
	Local    bool
	PeerName string
	TargetID strfmt.UUID
	Kind     kind.Kind
}

// Parse is a safe way to generate a Ref, as it will error if any of the input
// parameters are not as expected.
func Parse(uriString string) (*Ref, error) {
	uri, err := url.Parse(uriString)
	if err != nil {
		return nil, fmt.Errorf("invalid cref URI: %s", err)
	}

	pathSegments := strings.Split(uri.Path, "/")
	if len(pathSegments) != 3 {
		return nil, fmt.Errorf(
			"invalid cref URI: path must be of format '/{things,actions}/<uuid>', but got '%s'",
			uri.Path)
	}

	if ok := strfmt.IsUUID(pathSegments[2]); !ok {
		return nil, fmt.Errorf("invalid cref URI: 2nd path segment must be uuid, but got '%s'",
			pathSegments[2])
	}

	k, err := parseKind(pathSegments[1])
	if err != nil {
		return nil, fmt.Errorf("invalid cref URI: %s", err)
	}

	return &Ref{
		Local:    (uri.Host == "localhost"),
		PeerName: uri.Host,
		TargetID: strfmt.UUID(pathSegments[2]),
		Kind:     k,
	}, nil
}

// ParseSingleRef is a safe way to generate a Ref from a models.SingleRef, a
// helper construct that represents the API structure. It will error if any of
// the input parameters are not as expected.
func ParseSingleRef(singleRef *models.SingleRef) (*Ref, error) {
	return Parse(string(singleRef.NrDollarCref))
}

// New is a safe way to generate a Reference, as all required arguments must be
// set in the constructor fn
func New(peerName string, target strfmt.UUID, k kind.Kind) *Ref {
	return &Ref{
		Local:    (peerName == "localhost"),
		PeerName: peerName,
		TargetID: target,
		Kind:     k,
	}
}

func parseKind(kinds string) (kind.Kind, error) {
	switch kinds {
	case "things":
		return kind.Thing, nil
	case "actions":
		return kind.Action, nil
	default:
		return "", fmt.Errorf("invalid kind, expected 'things' or 'actions', but got '%s'", kinds)
	}
}

func (r *Ref) String() string {
	uri := url.URL{
		Host:   r.PeerName,
		Scheme: "weaviate",
		Path:   fmt.Sprintf("/%s/%s", pluralizeKindName(r.Kind), r.TargetID),
	}

	return uri.String()
}

func pluralizeKindName(k kind.Kind) string {
	return strings.ToLower(k.Name()) + "s"
}

// SingleRef converts the parsed Ref back into the API helper construct
// containing a stringified representation (URI format) of the Ref
func (r *Ref) SingleRef() *models.SingleRef {
	return &models.SingleRef{
		NrDollarCref: strfmt.URI(r.String()),
	}
}
