package clients

import (
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type metrics struct {
	remoteIndexRequests *prometheus.CounterVec
}

func newMetrics(prom *monitoring.PrometheusMetrics) *metrics {
	if prom == nil {
		return nil
	}

	return &metrics{
		remoteIndexRequests: prom.ClientRemoteIndexRequests,
	}
}

// TODO rename to do
func (m *metrics) remoteIndexRequestErr(destinationHost, method, endpointId string) {
	m.remoteIndexRequestInc(destinationHost, m.methodEndpointId(method, endpointId), "ERROR")
}

func (m *metrics) remoteIndexRequest(response *http.Response, endpointId string) {
	if response == nil || response.Request == nil {
		return
	}
	m.remoteIndexRequestInc(
		response.Request.Host,
		m.methodEndpointId(response.Request.Method, endpointId),
		strconv.Itoa(response.StatusCode))
}

func (m *metrics) remoteIndexRequestInc(destinationHost, endpointId, status string) {
	if m == nil {
		return
	}
	m.remoteIndexRequests.With(prometheus.Labels{
		"destination_host": destinationHost,
		"status":           status,
		"endpoint":         endpointId,
	}).Inc()
}

func (m *metrics) methodEndpointId(method, endpointId string) string {
	return method + " " + endpointId
}
