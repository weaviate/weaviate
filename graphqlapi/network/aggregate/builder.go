package aggregate

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/descriptions"
	"github.com/graphql-go/graphql"
)

// New Aggregate Builder to build PeerFields
func New(peerName string, schema schema.Schema) *Builder {
	return &Builder{
		peerName:        peerName,
		schema:          schema,
		existingClasses: map[string]*graphql.Object{},
	}
}

// Builder for Network -> Aggregate
type Builder struct {
	peerName        string
	schema          schema.Schema
	existingClasses map[string]*graphql.Object
}

// PeerField for Network -> Aggregate -> <Peer>
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
		Name:        fmt.Sprintf("WeaviateNetworkAggregate%sObj", b.peerName),
		Fields:      kinds,
		Description: fmt.Sprintf("%s%s", descriptions.NetworkAggregateWeaviateObjDesc, b.peerName),
	})

	field := &graphql.Field{
		Name:        fmt.Sprintf("%s%s", "Meta", b.peerName),
		Description: fmt.Sprintf("%s%s", descriptions.NetworkWeaviateDesc, b.peerName),
		Type:        object,
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return nil, fmt.Errorf("Network.Aggregate.<Peer> Resolver not implemented yet")
		},
	}
	return field, nil
}

func (b *Builder) buildKinds() (graphql.Fields, error) {
	fields := graphql.Fields{}

	if b.schema.Actions != nil && len(b.schema.Actions.Classes) > 0 {
		actions, err := b.buildKind(kind.ACTION_KIND)
		if err != nil {
			return nil, fmt.Errorf("could not build 'action' kind: %s", err)
		}

		fields["Actions"] = newActionsField(actions)
	}

	if b.schema.Things != nil && len(b.schema.Things.Classes) > 0 {
		things, err := b.buildKind(kind.THING_KIND)
		if err != nil {
			return nil, fmt.Errorf("could not build 'thing' kind: %s", err)
		}

		fields["Things"] = newThingsField(things)
	}

	return fields, nil
}

func newActionsField(actions *graphql.Object) *graphql.Field {
	return &graphql.Field{
		Name:        "WeaviateNetworkAggregateActions",
		Description: descriptions.NetworkAggregateActionsDesc,
		Type:        actions,
		Resolve:     passThroughResolver,
	}
}

func newThingsField(things *graphql.Object) *graphql.Field {
	return &graphql.Field{
		Name:        "WeaviateNetworkAggregateThings",
		Description: descriptions.NetworkAggregateThingsDesc,
		Type:        things,
		Resolve:     passThroughResolver,
	}
}

func (b *Builder) buildKind(k kind.Kind) (*graphql.Object, error) {
	// from here on we have legacy (unrefactored code). This method is the
	// transition

	switch k {
	case kind.ACTION_KIND:
		return BuildAggregateClasses(&b.schema, k, b.schema.Actions, &b.existingClasses, b.peerName)
	case kind.THING_KIND:
		return BuildAggregateClasses(&b.schema, k, b.schema.Things, &b.existingClasses, b.peerName)
	}

	return nil, fmt.Errorf("unrecognized kind '%s'", k)
}

func passThroughResolver(p graphql.ResolveParams) (interface{}, error) {
	return p.Source, nil
}
