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

package get

import (
	"fmt"
	"strings"

	"github.com/graphql-go/graphql"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/local/get/refclasses"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/network/common/peers"
	"github.com/sirupsen/logrus"
)

// Build the Local.Get part of the graphql tree
func Build(dbSchema *schema.Schema, peers peers.Peers, logger logrus.FieldLogger) (*graphql.Field, error) {
	getKinds := graphql.Fields{}

	if len(dbSchema.Actions.Classes) == 0 && len(dbSchema.Things.Classes) == 0 {
		return nil, fmt.Errorf("there are no Actions or Things classes defined yet")
	}

	knownClasses := map[string]*graphql.Object{}
	networkRefs := extractNetworkRefClassNames(dbSchema)
	knownRefClasses, err := refclasses.FromPeers(peers, networkRefs)
	if err != nil {
		msg := "an error occured while trying to build known network ref classes, " +
			"this kind of error won't block the graphql api, but it does mean that the mentioned refs " +
			"will not be available. This error is expected when the network is not ready yet. If so, " +
			"it should not reappear after a peer update"
		logger.WithField("action", "graphql_rebuild").WithError(err).Warning(msg)
	}

	if len(dbSchema.Actions.Classes) > 0 {
		localGetActions, err := buildGetClasses(dbSchema, kind.Action, dbSchema.Actions, &knownClasses, knownRefClasses, peers)
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
		localGetThings, err := buildGetClasses(dbSchema, kind.Thing, dbSchema.Things, &knownClasses, knownRefClasses, peers)
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
