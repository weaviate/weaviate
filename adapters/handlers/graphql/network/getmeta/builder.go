/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */package getmeta

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/creativesoftwarefdn/weaviate/entities/schema"
	"github.com/creativesoftwarefdn/weaviate/entities/schema/kind"
	"github.com/graphql-go/graphql"
)

// New GetMeta Builder to build PeerFields
func New(peerName string, schema schema.Schema) *Builder {
	return &Builder{
		peerName: peerName,
		schema:   schema,
	}
}

// Builder for Network -> GetMeta
type Builder struct {
	peerName string
	schema   schema.Schema
}

// PeerField for Network -> GetMeta -> <Peer>
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
		Name:        fmt.Sprintf("WeaviateNetworkGetMeta%sObj", b.peerName),
		Fields:      kinds,
		Description: fmt.Sprintf("%s%s", descriptions.NetworkGetMetaWeaviateObj, b.peerName),
	})

	field := &graphql.Field{
		Name:        fmt.Sprintf("%s%s", "Meta", b.peerName),
		Description: fmt.Sprintf("%s%s", descriptions.NetworkWeaviate, b.peerName),
		Type:        object,
		Resolve:     Resolve,
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
		Name:        "WeaviateNetworkGetMetaActions",
		Description: descriptions.NetworkGetMetaActions,
		Type:        actions,
	}
}

func newThingsField(things *graphql.Object) *graphql.Field {
	return &graphql.Field{
		Name:        "WeaviateNetworkGetMetaThings",
		Description: descriptions.NetworkGetMetaThings,
		Type:        things,
	}
}

func (b *Builder) buildKind(k kind.Kind) (*graphql.Object, error) {
	// from here on we have legacy (unrefactored code). This method is the
	// transition

	switch k {
	case kind.Action:
		return classFields(b.schema.Actions.Classes, k, b.peerName)
	case kind.Thing:
		return classFields(b.schema.Things.Classes, k, b.peerName)
	}

	return nil, fmt.Errorf("unrecognized kind '%s'", k)

}

func passThroughResolver(p graphql.ResolveParams) (interface{}, error) {
	return p.Source, nil
}
