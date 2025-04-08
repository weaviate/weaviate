//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package monitoring

import "github.com/prometheus/client_golang/prometheus"

// NoopRegisterer is a no-op Prometheus register.
var NoopRegisterer prometheus.Registerer = noopRegisterer{}

type noopRegisterer struct{}

func (n noopRegisterer) Register(_ prometheus.Collector) error { return nil }

func (n noopRegisterer) MustRegister(_ ...prometheus.Collector) {}

func (n noopRegisterer) Unregister(_ prometheus.Collector) bool { return true }
