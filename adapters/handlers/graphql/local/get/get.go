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
 */

package get

import (
	"fmt"
	"strings"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/entities/models"
	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/local/get/refclasses"
	"github.com/creativesoftwarefdn/weaviate/messages"
	"github.com/creativesoftwarefdn/weaviate/usecases/network/common/peers"
	"github.com/graphql-go/graphql"
)

// Build the Local.Get part of the graphql tree
func Build(dbSchema *schema.Schema, peers peers.Peers, logger *messages.Messaging) (*graphql.Field, error) {
	getKinds := graphql.Fields{}

	if len(dbSchema.Actions.Classes) == 0 && len(dbSchema.Things.Classes) == 0 {
		return nil, fmt.Errorf("there are no Actions or Things classes defined yet")
	}

	knownClasses := map[string]*graphql.Object{}
	networkRefs := extractNetworkRefClassNames(dbSchema)
	knownRefClasses, err := refclasses.FromPeers(peers, networkRefs)
	if err != nil {
		msg := fmt.Sprintf("an error occured while trying to build known network ref classes, "+
			"this kind of error won't block the graphql api, but it does mean that the mentioned refs "+
			"will not be available. This error is expected when the network is not ready yet. If so, "+
			"it should not reappear after a peer update: %s", err)
		logger.ErrorMessage(msg)
	}

	if len(dbSchema.Actions.Classes) > 0 {
		localGetActions, err := buildGetClasses(dbSchema, kind.ACTION_KIND, dbSchema.Actions, &knownClasses, knownRefClasses, peers)
		if err != nil {
			return nil, err
		}

		getKinds["Actions"] = &graphql.Field{
			Name:        "WeaviateLocalGetActions",
			Description: descriptions.LocalGetActions,
			Type:        localGetActions,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				// Does nothing; pass through the filters
				return p.Source, nil
			},
		}
	}

	if len(dbSchema.Things.Classes) > 0 {
		localGetThings, err := buildGetClasses(dbSchema, kind.THING_KIND, dbSchema.Things, &knownClasses, knownRefClasses, peers)
		if err != nil {
			return nil, err
		}

		getKinds["Things"] = &graphql.Field{
			Name:        "WeaviateLocalGetThings",
			Description: descriptions.LocalGetThings,
			Type:        localGetThings,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				// Does nothing; pass through the filters
				return p.Source, nil
			},
		}
	}

	field := graphql.Field{
		Name:        "WeaviateLocalGet",
		Description: descriptions.LocalGet,
		Type: graphql.NewObject(graphql.ObjectConfig{
			Name:        "WeaviateLocalGetObj",
			Fields:      getKinds,
			Description: descriptions.LocalGetObj,
		}),
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			return p.Source, nil
		},
	}

	return &field, nil
}

// Builds the classes below a Local -> Get -> (k kind.Kind)
func buildGetClasses(dbSchema *schema.Schema, k kind.Kind, semanticSchema *models.SemanticSchema,
	knownClasses *map[string]*graphql.Object, knownRefClasses refclasses.ByNetworkClass,
	peers peers.Peers) (*graphql.Object, error) {
	classFields := graphql.Fields{}
	kindName := strings.Title(k.Name())

	for _, class := range semanticSchema.Classes {
		classField, err := buildGetClass(dbSchema, k, class, knownClasses, knownRefClasses, peers)
		if err != nil {
			return nil, fmt.Errorf("Could not build class for %s", class.Class)
		}
		classFields[class.Class] = classField
	}

	classes := graphql.NewObject(graphql.ObjectConfig{
		Name:        fmt.Sprintf("WeaviateLocalGet%ssObj", kindName),
		Fields:      classFields,
		Description: fmt.Sprintf(descriptions.LocalGetThingsActionsObj, kindName),
	})

	return classes, nil
}
