package monitoring

import "github.com/prometheus/client_golang/prometheus"

var noop prometheus.Registerer = &NoopPrometheusRegistery{}

// NoopPrometheusRegistery is a no-op registry mainly used to disable metrics
// registery when monitoring is disabled.
type NoopPrometheusRegistery struct{}

func (n *NoopPrometheusRegistery) Register(prometheus.Collector) error {
	return nil
}

func (n *NoopPrometheusRegistery) MustRegister(...prometheus.Collector) {
}

func (n *NoopPrometheusRegistery) Unregister(prometheus.Collector) bool {
	return true
}
