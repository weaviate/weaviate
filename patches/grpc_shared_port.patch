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

32a33
> 	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
35a37,38
> 	"golang.org/x/net/http2"
> 	"golang.org/x/net/http2/h2c"
37a41
> 	"github.com/weaviate/weaviate/adapters/handlers/grpc"
68c72
< 		s.handler = configureAPI(s.api)
---
> 		s.handler,s.grpcServer, s.appState = configureAPI(s.api)
109a114,115
> 	grpcServer   *grpc.GRPCServer
> 	appState *state.State
146c152
< 	s.handler = configureAPI(api)
---
> 	s.handler, s.grpcServer, s.appState = configureAPI(api)
162a169
> 
223c230,239
< 		httpServer.Handler = s.handler
---
> 		h2s := &http2.Server{}
> 		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
> 			if r.ProtoMajor == 2 && r.Header.Get("Content-Type") == "application/grpc" {
> 				s.grpcServer.ServeHTTP(w, r)
> 			} else {
> 				s.handler.ServeHTTP(w, r)
> 			}
> 		})
> 	
> 		httpServer.Handler = h2c.NewHandler(handler, h2s)
226a243,244
> 
> 
251c269
< 		httpsServer.Handler = s.handler
---
> 		httpsServer.Handler = makeSharedPortHandlerFunc(s.grpcServer, s.handler)
317a336,348
> 		h2s := &http2.Server{}
> 		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
> 			if r.ProtoMajor == 2 && r.Header.Get("Content-Type") == "application/grpc" {
> 				fmt.Println("grpc")
> 				s.grpcServer.ServeHTTP(w, r)
> 			} else {
> 				fmt.Println("http")
> 				s.handler.ServeHTTP(w, r)
> 			}
> 		})
> 	
> 		httpsServer.Handler = h2c.NewHandler(handler, h2s)
> 
518a550
> 
