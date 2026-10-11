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

import "sync"

// ServeAndShutdown runs Serve, then the api's ServerShutdown hook exactly once.
// The generated handleShutdown skips ServerShutdown when an HTTP server does not
// drain within GracefulTimeout, which would leave the cluster without a graceful
// departure and the database open. Call it after ConfigureAPI, which sets the hook.
func (s *Server) ServeAndShutdown() error {
	serverShutdown := sync.OnceFunc(s.api.ServerShutdown)
	s.api.ServerShutdown = serverShutdown

	if err := s.Serve(); err != nil {
		return err
	}
	serverShutdown()
	return nil
}
