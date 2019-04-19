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

package rest

import (
	"net/http"

	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/graphiql"
	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/rest/state"
	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/rest/swagger_middleware"
	"github.com/creativesoftwarefdn/weaviate/lib/feature_flags"
	"github.com/rs/cors"
	"github.com/sirupsen/logrus"
)

// The middleware configuration is for the handler executors. These do not apply to the swagger.json document.
// The middleware executes after routing but before authentication, binding and validation
//
// we are setting the middlewares from within configureAPI, as we need access
// to some resources which are not exposed
func makeSetupMiddlewares(appState *state.State) func(http.Handler) http.Handler {
	return func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			appState.AnonymousAccess.Middleware(handler).ServeHTTP(w, r)
		})
	}

}

// The middleware configuration happens before anything, this middleware also applies to serving the swagger.json document.
// So this is a good place to plug in a panic handling middleware, logging and metrics
// Contains "x-api-key", "x-api-token" for legacy reasons, older interfaces might need these headers.
func makeSetupGlobalMiddleware(appState *state.State) func(http.Handler) http.Handler {
	return func(handler http.Handler) http.Handler {
		handleCORS := cors.New(cors.Options{
			OptionsPassthrough: true,
			AllowedMethods:     []string{"POST", "PUT", "DELETE", "GET", "PATCH"},
		}).Handler
		handler = handleCORS(handler)

		if feature_flags.EnableDevUI {
			handler = graphiql.AddMiddleware(handler)
			handler = swagger_middleware.AddMiddleware([]byte(SwaggerJSON), handler)
		}

		handler = makeAddLogging(appState.Logger)(handler)
		handler = addPreflight(handler)

		return handler
	}
}

func makeAddLogging(logger logrus.FieldLogger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			logger.
				WithField("action", "restapi_request").
				WithField("method", r.Method).
				WithField("url", r.URL).
				Debug("received HTTP request")
			next.ServeHTTP(w, r)
		})
	}
}

func addPreflight(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		if r.Method == "OPTIONS" {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "*")
			w.Header().Set("Access-Control-Allow-Headers", "*")
			return
		}

		next.ServeHTTP(w, r)
	})
}
