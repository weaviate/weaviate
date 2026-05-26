//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package rest

import (
	"errors"

	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	schemaops "github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	tokenizeops "github.com/weaviate/weaviate/adapters/handlers/rest/operations/tokenize"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

func setupTokenizeHandlers(api *operations.WeaviateAPI, schemaManager *schemaUC.Manager, logger logrus.FieldLogger) {
	api.TokenizeTokenizeHandler = tokenizeops.TokenizeHandlerFunc(
		func(params tokenizeops.TokenizeParams, principal *models.Principal) middleware.Responder {
			return genericTokenize(params, logger)
		})

	api.SchemaSchemaObjectsPropertiesTokenizeHandler = schemaops.SchemaObjectsPropertiesTokenizeHandlerFunc(
		func(params schemaops.SchemaObjectsPropertiesTokenizeParams, principal *models.Principal) middleware.Responder {
			return propertyTokenize(params, principal, schemaManager, logger)
		})
}

func genericTokenize(params tokenizeops.TokenizeParams, logger logrus.FieldLogger) middleware.Responder {
	result, err := schemaUC.GenericTokenize(
		*params.Body.Text,
		*params.Body.Tokenization,
		params.Body.AnalyzerConfig,
		params.Body.Stopwords,
		params.Body.StopwordPresets,
	)
	if err != nil {
		// Mirror the propertyTokenize Kind switch so a future TokenizeErrInternal
		// path inside schemaUC.GenericTokenize cannot be silently misclassified
		// as 422. Today every error path is TokenizeErrInvalid; the default arm
		// is defensive for that future case (and for the structurally-impossible
		// TokenizeErrNotFound — no class lookup happens here, hence no
		// NewTokenizeNotFound responder is generated for this operation).
		var te *schemaUC.TokenizeError
		if errors.As(err, &te) {
			switch te.Kind {
			case schemaUC.TokenizeErrInvalid:
				return tokenizeops.NewTokenizeUnprocessableEntity().WithPayload(&models.ErrorResponse{
					Error: []*models.ErrorResponseErrorItems0{{Message: te.Msg}},
				})
			default:
				logger.WithField("action", "generic_tokenize").Errorf("tokenize: %v", err)
				return tokenizeops.NewTokenizeInternalServerError().WithPayload(&models.ErrorResponse{
					Error: []*models.ErrorResponseErrorItems0{{Message: te.Msg}},
				})
			}
		}
		logger.WithField("action", "generic_tokenize").Errorf("tokenize: %v", err)
		return tokenizeops.NewTokenizeInternalServerError().WithPayload(&models.ErrorResponse{
			Error: []*models.ErrorResponseErrorItems0{{Message: err.Error()}},
		})
	}
	return tokenizeops.NewTokenizeOK().WithPayload(&models.TokenizeResponse{
		Indexed: result.Indexed,
		Query:   result.Query,
	})
}

func propertyTokenize(params schemaops.SchemaObjectsPropertiesTokenizeParams,
	principal *models.Principal, schemaManager *schemaUC.Manager, logger logrus.FieldLogger,
) middleware.Responder {
	className := schema.UppercaseClassName(params.ClassName)

	// Resolve alias before authorization so authz uses the real collection name
	// for permissions and error UX (matches Handler.ShardsStatus).
	if resolved := schemaManager.ResolveAlias(className); resolved != "" {
		className = resolved
	}

	err := schemaManager.Authorizer.Authorize(
		params.HTTPRequest.Context(), principal, authorization.READ,
		authorization.CollectionsMetadata(className)...,
	)
	if err != nil {
		if errors.As(err, &authzerrors.Forbidden{}) {
			return schemaops.NewSchemaObjectsPropertiesTokenizeForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		}
		return schemaops.NewSchemaObjectsPropertiesTokenizeInternalServerError().
			WithPayload(errPayloadFromSingleErr(err))
	}

	class := schemaManager.ReadOnlyClass(className)
	if class == nil {
		return schemaops.NewSchemaObjectsPropertiesTokenizeNotFound()
	}

	result, terr := schemaUC.PropertyTokenize(class, params.PropertyName, *params.Body.Text)
	if terr != nil {
		var te *schemaUC.TokenizeError
		if errors.As(terr, &te) {
			switch te.Kind {
			case schemaUC.TokenizeErrNotFound:
				return schemaops.NewSchemaObjectsPropertiesTokenizeNotFound()
			case schemaUC.TokenizeErrInvalid:
				return schemaops.NewSchemaObjectsPropertiesTokenizeUnprocessableEntity().WithPayload(&models.ErrorResponse{
					Error: []*models.ErrorResponseErrorItems0{{Message: te.Msg}},
				})
			default:
				logger.WithField("action", "property_tokenize").Error(terr)
				return schemaops.NewSchemaObjectsPropertiesTokenizeInternalServerError().WithPayload(&models.ErrorResponse{
					Error: []*models.ErrorResponseErrorItems0{{Message: te.Msg}},
				})
			}
		}
		logger.WithField("action", "property_tokenize").Error(terr)
		return schemaops.NewSchemaObjectsPropertiesTokenizeInternalServerError().WithPayload(&models.ErrorResponse{
			Error: []*models.ErrorResponseErrorItems0{{Message: terr.Error()}},
		})
	}

	return schemaops.NewSchemaObjectsPropertiesTokenizeOK().WithPayload(&models.TokenizeResponse{
		Indexed: result.Indexed,
		Query:   result.Query,
	})
}
