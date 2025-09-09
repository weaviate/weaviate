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

package replica

import (
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// Histogram buckets for different operation types
var (
	writeDurationBuckets      = prometheus.ExponentialBuckets(0.001, 2, 18)   // ~1ms to 136s
	readDurationBuckets       = prometheus.ExponentialBuckets(0.00025, 2, 18) // ~0.25ms to 32s
	readRepairDurationBuckets = prometheus.ExponentialBuckets(0.001, 2, 18)   // ~1ms to 136s
)

type Metrics struct {
	monitoring bool

	// Write Metrics
	writesSucceedAll  prometheus.Counter
	writesSucceedSome prometheus.Counter
	writesFailed      prometheus.Counter

	// Read Metrics
	readsSucceedAll  prometheus.Counter
	readsSucceedSome prometheus.Counter
	readsFailed      prometheus.Counter

	// Read Repair Metrics
	readRepairCount   prometheus.Counter
	readRepairFailure prometheus.Counter
	readRepairSuccess prometheus.Counter

	// Histograms
	writeDuration      prometheus.Histogram
	readDuration       prometheus.Histogram
	readRepairDuration prometheus.Histogram
}

func NewMetrics(prom *monitoring.PrometheusMetrics) (*Metrics, error) {
	m := &Metrics{}

	if prom == nil {
		return m, nil
	}
	m.monitoring = true

	if prom.Registerer == nil {
		prom.Registerer = prometheus.DefaultRegisterer
	}

	var err error

	// Write metrics
	m.writesSucceedAll, err = newCounter(prom.Registerer,
		"replication_coordinator_writes_succeed_all", "Count of requests succeeding a write to all replicas")
	if err != nil {
		return nil, err
	}
	m.writesSucceedSome, err = newCounter(prom.Registerer,
		"replication_coordinator_writes_succeed_some", "Count of requests succeeding a write to some replicas > CL but less than all")
	if err != nil {
		return nil, err
	}
	m.writesFailed, err = newCounter(prom.Registerer,
		"replication_coordinator_writes_failed", "Count of requests failing due to consistency level")
	if err != nil {
		return nil, err
	}

	// Read metrics
	m.readsSucceedAll, err = newCounter(prom.Registerer,
		"replication_coordinator_reads_succeed_all", "Count of requests succeeding a read from CL replicas")
	if err != nil {
		return nil, err
	}
	m.readsSucceedSome, err = newCounter(prom.Registerer,
		"replication_coordinator_reads_succeed_some", "Count of requests succeeding a read from some replicas < CL but more than zero")
	if err != nil {
		return nil, err
	}
	m.readsFailed, err = newCounter(prom.Registerer,
		"replication_coordinator_reads_failed", "Count of requests failing due to read from replicas")
	if err != nil {
		return nil, err
	}

	// Read repair metrics
	m.readRepairCount, err = newCounter(prom.Registerer,
		"replication_read_repair_count", "Count of read repairs started")
	if err != nil {
		return nil, err
	}
	m.readRepairFailure, err = newCounter(prom.Registerer,
		"replication_read_repair_failure", "Count of read repairs failed")
	if err != nil {
		return nil, err
	}
	m.readRepairSuccess, err = newCounter(prom.Registerer,
		"replication_read_repair_success", "Count of read repairs succeeded")
	if err != nil {
		return nil, err
	}

	// Histograms
	m.writeDuration, err = newHistogram(prom.Registerer,
		"replication_coordinator_writes_duration", "Duration of write operations to replicas", writeDurationBuckets)
	if err != nil {
		return nil, err
	}
	m.readDuration, err = newHistogram(prom.Registerer,
		"replication_coordinator_reads_duration", "Duration of read operations from replicas", readDurationBuckets)
	if err != nil {
		return nil, err
	}
	m.readRepairDuration, err = newHistogram(prom.Registerer,
		"replication_read_repair_duration", "Duration of read repair operations", readRepairDurationBuckets)
	if err != nil {
		return nil, err
	}

	return m, nil
}

func newCounter(reg prometheus.Registerer, name, help string) (prometheus.Counter, error) {
	c := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "weaviate",
		Name:      name,
		Help:      help,
	})
	if err := reg.Register(c); err != nil {
		var e prometheus.AlreadyRegisteredError
		if errors.As(err, &e) {
			if counter, ok := e.ExistingCollector.(prometheus.Counter); ok {
				return counter, nil
			}
			return nil, fmt.Errorf("metric %s already registered but not as a Counter", name)
		}
		return nil, err
	}
	return c, nil
}

func newHistogram(reg prometheus.Registerer, name, help string, buckets []float64) (prometheus.Histogram, error) {
	h := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "weaviate",
		Name:      name,
		Help:      help,
		Buckets:   buckets,
	})
	if err := reg.Register(h); err != nil {
		var e prometheus.AlreadyRegisteredError
		if errors.As(err, &e) {
			return e.ExistingCollector.(prometheus.Histogram), nil
		}
		return nil, err
	}
	return h, nil
}

// Write increment methods

func (m *Metrics) IncWritesSucceedAll() {
	if m.monitoring {
		m.writesSucceedAll.Inc()
	}
}

func (m *Metrics) IncWritesSucceedSome() {
	if m.monitoring {
		m.writesSucceedSome.Inc()
	}
}

func (m *Metrics) IncWritesFailed() {
	if m.monitoring {
		m.writesFailed.Inc()
	}
}

// Read increment methods

func (m *Metrics) IncReadsSucceedAll() {
	if m.monitoring {
		m.readsSucceedAll.Inc()
	}
}

func (m *Metrics) IncReadsSucceedSome() {
	if m.monitoring {
		m.readsSucceedSome.Inc()
	}
}

func (m *Metrics) IncReadsFailed() {
	if m.monitoring {
		m.readsFailed.Inc()
	}
}

// Read repair increment methods

func (m *Metrics) IncReadRepairCount() {
	if m.monitoring {
		m.readRepairCount.Inc()
	}
}

func (m *Metrics) IncReadRepairFailure() {
	if m.monitoring {
		m.readRepairFailure.Inc()
	}
}

func (m *Metrics) IncReadRepairSuccess() {
	if m.monitoring {
		m.readRepairSuccess.Inc()
	}
}

// Duration observation methods

func (m *Metrics) ObserveWriteDuration(d time.Duration) {
	if m.monitoring {
		m.writeDuration.Observe(d.Seconds())
	}
}

func (m *Metrics) ObserveReadDuration(d time.Duration) {
	if m.monitoring {
		m.readDuration.Observe(d.Seconds())
	}
}

func (m *Metrics) ObserveReadRepairDuration(d time.Duration) {
	if m.monitoring {
		m.readRepairDuration.Observe(d.Seconds())
	}
}
