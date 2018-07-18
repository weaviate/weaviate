package restapi

import (
	"crypto/tls"
	"net/http"

	errors "github.com/go-openapi/errors"
	runtime "github.com/go-openapi/runtime"
	middleware "github.com/go-openapi/runtime/middleware"

	"github.com/creativesoftwarefdn/weaviate/genesis/restapi/operations"

   log "github.com/sirupsen/logrus"
)

//go:generate swagger generate server --target .. --name weaviate-genesis --spec ../openapi-spec.json --default-scheme https

func configureFlags(api *operations.WeaviateGenesisAPI) {
	// api.CommandLineOptionsGroups = []swag.CommandLineOptionsGroup{ ... }
}

func configureAPI(api *operations.WeaviateGenesisAPI) http.Handler {
	// configure the api here
	api.ServeError = errors.ServeError

	// Set your custom logger if needed. Default one is log.Printf
	// Expected interface func(string, ...interface{})
	//
	// Example:
	api.Logger = log.Infof

	api.JSONConsumer = runtime.JSONConsumer()

	api.JSONProducer = runtime.JSONProducer()

	api.GenesisPeersLeaveHandler = operations.GenesisPeersLeaveHandlerFunc(func(params operations.GenesisPeersLeaveParams) middleware.Responder {
		return middleware.NotImplemented("operation .GenesisPeersLeave has not yet been implemented")
	})
	api.GenesisPeersPingHandler = operations.GenesisPeersPingHandlerFunc(func(params operations.GenesisPeersPingParams) middleware.Responder {
		return middleware.NotImplemented("operation .GenesisPeersPing has not yet been implemented")
	})
	api.GenesisPeersRegisterHandler = operations.GenesisPeersRegisterHandlerFunc(func(params operations.GenesisPeersRegisterParams) middleware.Responder {
		return middleware.NotImplemented("operation .GenesisPeersRegister has not yet been implemented")
	})

	api.ServerShutdown = func() {}

	return setupGlobalMiddleware(api.Serve(setupMiddlewares))
}

// The TLS configuration before HTTPS server starts.
func configureTLS(tlsConfig *tls.Config) {
	// Make all necessary changes to the TLS configuration here.
}

// As soon as server is initialized but not run yet, this function will be called.
// If you need to modify a config, store server instance to stop it individually later, this is the place.
// This function can be called multiple times, depending on the number of serving schemes.
// scheme value will be set accordingly: "http", "https" or "unix"
func configureServer(s *http.Server, scheme, addr string) {
}

// The middleware configuration is for the handler executors. These do not apply to the swagger.json document.
// The middleware executes after routing but before authentication, binding and validation
func setupMiddlewares(handler http.Handler) http.Handler {
	return handler
}

// The middleware configuration happens before anything, this middleware also applies to serving the swagger.json document.
// So this is a good place to plug in a panic handling middleware, logging and metrics
func setupGlobalMiddleware(handler http.Handler) http.Handler {
	return handler
}
