//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package network_get

import (
	"fmt"

	"github.com/graphql-go/graphql"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
)

// New Get Builder to build PeerFields
func New(peerName string, schema schema.Schema) *Builder {
	return &Builder{
		peerName:        peerName,
		schema:          schema,
		existingClasses: map[string]*graphql.Object{},
	}
}

// Builder for Network -> Get
type Builder struct {
	peerName        string
	schema          schema.Schema
	existingClasses map[string]*graphql.Object
}

// PeerField for Network -> Get -> <Peer>
func (b *Builder) PeerField() (*graphql.Field, error) {
	kinds, err := b.buildKinds()
	if err != nil {
		return nil, fmt.Errorf("could not build kinds for peer '%s': %s", b.peerName, err)
	}

	if len(kinds) == 0 {
		// if we didn't find a single class for all kinds, it's essentially the
		// same as if this peer didn't exist
		return nil, nil
	}

	object := graphql.NewObject(graphql.ObjectConfig{
		Name:        fmt.Sprintf("WeaviateNetworkGet%sObj", b.peerName),
		Fields:      kinds,
		Description: fmt.Sprintf("%s%s", descriptions.NetworkGetWeaviateObj, b.peerName),
	})

	field := &graphql.Field{
		Name:        fmt.Sprintf("%s%s", "Meta", b.peerName),
		Description: fmt.Sprintf("%s%s", descriptions.NetworkWeaviate, b.peerName),
		Type:        object,
		Resolve:     NetworkGetInstanceResolve,
	}
	return field, nil
}

func (b *Builder) buildKinds() (graphql.Fields, error) {
	fields := graphql.Fields{}

	if b.schema.Actions != nil && len(b.schema.Actions.Classes) > 0 {
		actions, err := b.buildKind(kind.Action)
		if err != nil {
			return nil, fmt.Errorf("could not build 'action' kind: %s", err)
		}

		fields["Actions"] = newActionsField(actions)
	}

	if b.schema.Things != nil && len(b.schema.Things.Classes) > 0 {
		things, err := b.buildKind(kind.Thing)
		if err != nil {
			return nil, fmt.Errorf("could not build 'thing' kind: %s", err)
		}

		fields["Things"] = newThingsField(things)
	}

	return fields, nil
}

func newActionsField(actions *graphql.Object) *graphql.Field {
	return &graphql.Field{
		Name:        "WeaviateNetworkGetActions",
		Description: descriptions.NetworkGetActions,
		Type:        actions,
		Resolve:     passThroughResolver,
	}
}

func newThingsField(things *graphql.Object) *graphql.Field {
	return &graphql.Field{
		Name:        "WeaviateNetworkGetThings",
		Description: descriptions.NetworkGetThings,
		Type:        things,
		Resolve:     passThroughResolver,
	}
}

func (b *Builder) buildKind(k kind.Kind) (*graphql.Object, error) {
	// from here on we have legacy (unrefactored code). This method is the
	// transition

	switch k {
	case kind.Action:
		return ActionClassFieldsFromSchema(&b.schema, &b.existingClasses, b.peerName)
	case kind.Thing:
		return ThingClassFieldsFromSchema(&b.schema, &b.existingClasses, b.peerName)
	}

	return nil, fmt.Errorf("unrecognized kind '%s'", k)
}

func passThroughResolver(p graphql.ResolveParams) (interface{}, error) {
	return p.Source, nil
}
