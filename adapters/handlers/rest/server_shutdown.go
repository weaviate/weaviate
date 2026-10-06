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
	"net/http"
	"sync"
)

// ServeAndShutdown runs Serve and closes servers still busy past GracefulTimeout,
// failing handlers blocked on slow clients. It then runs ServerShutdown exactly once,
// which the generated handleShutdown would skip. Call it after ConfigureAPI.
func (s *Server) ServeAndShutdown() error {
	serverShutdown := sync.OnceFunc(s.api.ServerShutdown)
	s.api.ServerShutdown = serverShutdown

	// Serve calls configureServer for every listener before it starts serving.
	var servers []*http.Server
	configure := configureServer
	configureServer = func(hs *http.Server, scheme, addr string) {
		configure(hs, scheme, addr)
		servers = append(servers, hs)
	}
	defer func() { configureServer = configure }()

	if err := s.Serve(); err != nil {
		return err
	}
	for _, hs := range servers {
		if err := hs.Close(); err != nil {
			s.Logf("HTTP server Close: %v", err)
		}
	}
	serverShutdown()
	return nil
}
