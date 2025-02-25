package monitoring

import "github.com/prometheus/client_golang/prometheus"

// NoopRegisterer is a no-op Prometheus register.
var NoopRegisterer prometheus.Registerer = noopRegisterer{}

type noopRegisterer struct{}

func (n noopRegisterer) Register(_ prometheus.Collector) error { return nil }

func (n noopRegisterer) MustRegister(_ ...prometheus.Collector) {}

func (n noopRegisterer) Unregister(_ prometheus.Collector) bool { return true }
