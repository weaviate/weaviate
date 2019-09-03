//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package validation

import (
	"context"
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/network/common/peers"
	"github.com/semi-technologies/weaviate/usecases/network/crossrefs"
)

type getRepo interface {
	GetThing(context.Context, strfmt.UUID, *models.Thing) error
	GetAction(context.Context, strfmt.UUID, *models.Action) error
}

type exists func(context.Context, kind.Kind, strfmt.UUID) (bool, error)

type peerLister interface {
	ListPeers() (peers.Peers, error)
}

const (
	// ErrorMissingActionThings message
	ErrorMissingActionThings string = "no things, object and subject, are added. Add 'things' by using the 'things' key in the root of the JSON"
	// ErrorMissingActionThingsObject message
	ErrorMissingActionThingsObject string = "no object-thing is added. Add the 'object' inside the 'things' part of the JSON"
	// ErrorMissingActionThingsSubject message
	ErrorMissingActionThingsSubject string = "no subject-thing is added. Add the 'subject' inside the 'things' part of the JSON"
	// ErrorMissingActionThingsObjectLocation message
	ErrorMissingActionThingsObjectLocation string = "no 'locationURL' is found in the object-thing. Add the 'locationURL' inside the 'object-thing' part of the JSON"
	// ErrorMissingActionThingsObjectType message
	ErrorMissingActionThingsObjectType string = "no 'type' is found in the object-thing. Add the 'type' inside the 'object-thing' part of the JSON"
	// ErrorInvalidActionThingsObjectType message
	ErrorInvalidActionThingsObjectType string = "object-thing requires one of the following values in 'type': '%s', '%s' or '%s'"
	// ErrorMissingActionThingsSubjectLocation message
	ErrorMissingActionThingsSubjectLocation string = "no 'locationURL' is found in the subject-thing. Add the 'locationURL' inside the 'subject-thing' part of the JSON"
	// ErrorMissingActionThingsSubjectType message
	ErrorMissingActionThingsSubjectType string = "no 'type' is found in the subject-thing. Add the 'type' inside the 'subject-thing' part of the JSON"
	// ErrorInvalidActionThingsSubjectType message
	ErrorInvalidActionThingsSubjectType string = "subject-thing requires one of the following values in 'type': '%s', '%s' or '%s'"
	// ErrorMissingClass message
	ErrorMissingClass string = "the given class is empty"
	// ErrorMissingContext message
	ErrorMissingContext string = "the given context is empty"
	// ErrorNoExternalCredentials message
	ErrorNoExternalCredentials string = "no credentials available for the Weaviate instance for %s given in the %s"
	// ErrorExternalNotFound message
	ErrorExternalNotFound string = "given statuscode of '%s' is '%d', but 200 was expected for LocationURL given in the %s"
	// ErrorInvalidCRefType message
	ErrorInvalidCRefType string = "'cref' type '%s' does not exists"
	// ErrorNotFoundInDatabase message
	ErrorNotFoundInDatabase string = "error finding the '%s' in the database: '%s' at %s"
)

type Validator struct {
	schema     schema.Schema
	exists     exists
	peerLister peerLister
	config     *config.WeaviateConfig
}

func New(schema schema.Schema, exists exists, peerLister peerLister,
	config *config.WeaviateConfig) *Validator {
	return &Validator{
		schema:     schema,
		exists:     exists,
		peerLister: peerLister,
		config:     config,
	}
}

func (v *Validator) Thing(ctx context.Context, thing *models.Thing) error {
	if err := validateClass(thing.Class); err != nil {
		return err
	}

	return v.properties(ctx, kind.Thing, thing)
}

func (v *Validator) Action(ctx context.Context, action *models.Action) error {
	if err := validateClass(action.Class); err != nil {
		return err
	}

	return v.properties(ctx, kind.Action, action)
}

func validateClass(class string) error {
	// If the given class is empty, return an error
	if class == "" {
		return fmt.Errorf(ErrorMissingClass)
	}

	// No error
	return nil
}

// validateRefType validates the reference type with one of the existing reference types
func validateRefType(s string) bool {
	return (s == "things" || s == "actions")
}

// ValidateSingleRef validates a single ref based on location URL and existence of the object in the database
func (v *Validator) ValidateSingleRef(ctx context.Context, cref *models.SingleRef,
	errorVal string) error {

	ref, err := crossref.ParseSingleRef(cref)
	if err != nil {
		return fmt.Errorf("invalid reference: %s", err)
	}

	if !ref.Local {
		return v.validateNetworkRef(ref)
	}

	return v.validateLocalRef(ctx, ref, errorVal)
}

func (v *Validator) validateLocalRef(ctx context.Context, ref *crossref.Ref, errorVal string) error {
	// Check whether the given Object exists in the DB
	var err error

	ok, err := v.exists(ctx, ref.Kind, ref.TargetID)
	if err != nil {
		return err
	}

	if !ok {
		return fmt.Errorf(ErrorNotFoundInDatabase, ref.Kind.Name(), err, errorVal)
	}

	return nil
}

func (v *Validator) validateNetworkRef(ref *crossref.Ref) error {
	// Network ref
	peers, err := v.peerLister.ListPeers()
	if err != nil {
		return fmt.Errorf("could not validate network reference: could not list network peers: %s", err)
	}

	_, err = peers.RemoteKind(crossrefs.NetworkKind{Kind: ref.Kind, ID: ref.TargetID, PeerName: ref.PeerName})
	if err != nil {
		return fmt.Errorf("invalid network reference: %s", err)
	}

	return nil
}

func (v *Validator) ValidateMultipleRef(ctx context.Context, refs models.MultipleRef,
	errorVal string) error {
	if refs == nil {
		return nil
	}

	for _, ref := range refs {
		err := v.ValidateSingleRef(ctx, ref, errorVal)
		if err != nil {
			return err
		}
	}
	return nil
}
