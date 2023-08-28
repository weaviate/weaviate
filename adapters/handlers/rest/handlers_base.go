//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package rest

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/runtime/middleware"
	"github.com/weaviate/weaviate/adapters/handlers/grpc"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/entities/models"
)

type basePathResponder func(http.ResponseWriter, runtime.Producer)

func (c basePathResponder) WriteResponse(w http.ResponseWriter, p runtime.Producer) {
	c(w, p)
}

func setupBasePathHandler(api *operations.WeaviateAPI, grpcServer *grpc.GRPCServer) {
	api.WeaviateBaseHandler = operations.WeaviateBaseHandlerFunc(func(wbp operations.WeaviateBaseParams, p *models.Principal) middleware.Responder {
		return basePathResponder(func(w http.ResponseWriter, _ runtime.Producer) {
			if wbp.HTTPRequest.ProtoMajor == 2 && strings.Contains(wbp.HTTPRequest.Header.Get("Content-Type"), "application/grpc") {
				fmt.Printf("---Running grpc handler\n")
				grpcServer.ServeHTTP(w, wbp.HTTPRequest)
			} else {
				fmt.Printf("---Running http handler\n")
				w.Header().Add("Location", "/v1")
				w.WriteHeader(http.StatusMovedPermanently)
				w.Write([]byte(`{"links":{"href":"/v1","name":"api v1","documentationHref":` +
					`"https://weaviate.io/developers/weaviate/current/"}}`))
				return
			}
		})
	})
}
