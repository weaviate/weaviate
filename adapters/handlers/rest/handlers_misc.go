//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
//  DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package rest

import (
	"fmt"

	middleware "github.com/go-openapi/runtime/middleware"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/operations"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/operations/meta"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/operations/p2_p"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/network"
	"github.com/semi-technologies/weaviate/usecases/network/common/peers"
	"github.com/semi-technologies/weaviate/usecases/telemetry"
)

type schemaManager interface {
	GetSchema(principal *models.Principal) (schema.Schema, error)
	GetSchemaSkipAuth() schema.Schema
}

func setupMiscHandlers(api *operations.WeaviateAPI, requestsLog *telemetry.RequestsLog,
	serverConfig *config.WeaviateConfig, network network.Network, schemaManager schemaManager) {
	api.MetaMetaGetHandler = meta.MetaGetHandlerFunc(func(params meta.MetaGetParams, principal *models.Principal) middleware.Responder {
		s, err := schemaManager.GetSchema(principal)
		if err != nil {
			return meta.NewMetaGetForbidden().WithPayload(errPayloadFromSingleErr(err))
		}

		databaseSchema := schema.HackFromDatabaseSchema(s)

		// Create response object
		metaResponse := &models.Meta{}

		// Set the response object's values
		metaResponse.Hostname = serverConfig.GetHostAddress()
		metaResponse.ActionsSchema = databaseSchema.ActionSchema.Schema
		metaResponse.ThingsSchema = databaseSchema.ThingSchema.Schema

		// Register the request
		go func() {
			requestsLog.Register(telemetry.TypeREST, telemetry.LocalQueryMeta)
		}()
		return meta.NewMetaGetOK().WithPayload(metaResponse)
	})

	api.P2PP2pGenesisUpdateHandler = p2_p.P2pGenesisUpdateHandlerFunc(func(params p2_p.P2pGenesisUpdateParams) middleware.Responder {
		newPeers := make([]peers.Peer, 0)

		for _, genesisPeer := range params.Peers {
			peer := peers.Peer{
				ID:         genesisPeer.ID,
				Name:       genesisPeer.Name,
				URI:        genesisPeer.URI,
				SchemaHash: genesisPeer.SchemaHash,
			}

			newPeers = append(newPeers, peer)
		}

		err := network.UpdatePeers(newPeers)

		if err == nil {
			// Register the request
			go func() {
				requestsLog.Register(telemetry.TypeREST, telemetry.NetworkQueryMeta)
			}()
			return p2_p.NewP2pGenesisUpdateOK()
		}
		return p2_p.NewP2pGenesisUpdateInternalServerError()
	})

	api.P2PP2pHealthHandler = p2_p.P2pHealthHandlerFunc(func(params p2_p.P2pHealthParams) middleware.Responder {
		// For now, always just return success.
		return middleware.NotImplemented("operation P2PP2pHealth has not yet been implemented")
	})

	api.GetWellKnownOpenidConfigurationHandler = operations.GetWellKnownOpenidConfigurationHandlerFunc(
		func(params operations.GetWellKnownOpenidConfigurationParams, principal *models.Principal) middleware.Responder {
			if !serverConfig.Config.Authentication.OIDC.Enabled {
				return operations.NewGetWellKnownOpenidConfigurationNotFound()
			}

			target := fmt.Sprintf("%s/.well-known/openid-configuration", serverConfig.Config.Authentication.OIDC.Issuer)
			clientID := serverConfig.Config.Authentication.OIDC.ClientID
			body := &operations.GetWellKnownOpenidConfigurationOKBody{
				Href:     target,
				ClientID: clientID,
			}

			return operations.NewGetWellKnownOpenidConfigurationOK().WithPayload(body)
		})
}
