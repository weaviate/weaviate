//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/usecases/monitoring"
)

var defaultDurationBuckets = prometheus.ExponentialBuckets(0.01, 2, 16) // 10ms to ~5min

type Metrics struct {
	logger                logrus.FieldLogger
	monitoring            bool
	batchTime             prometheus.ObserverVec
	batchDeleteTime       prometheus.ObserverVec
	batchCount            prometheus.Counter
	batchCountBytes       prometheus.Counter
	objectTime            prometheus.ObserverVec
	startupDurations      prometheus.ObserverVec
	filteredVectorFilter  prometheus.Observer
	filteredVectorVector  prometheus.Observer
	filteredVectorObjects prometheus.Observer
	filteredVectorSort    prometheus.Observer
	grouped               bool
	baseMetrics           *monitoring.PrometheusMetrics

	shardsCount                       *prometheus.GaugeVec
	shardStatusUpdateDurationsSeconds *prometheus.HistogramVec

	// async replication metrics
	asyncReplicationGoroutinesRunning prometheus.Gauge

	asyncReplicationHashTreeInitRunning      prometheus.Gauge
	asyncReplicationHashTreeInitFailureCount prometheus.Counter
	asyncReplicationHashTreeInitDuration     prometheus.Histogram

	asyncReplicationIterationCount        prometheus.Counter
	asyncReplicationIterationFailureCount prometheus.Counter
	asyncReplicationIterationDuration     prometheus.Histogram

	asyncReplicationPropagationCount        prometheus.Counter
	asyncReplicationPropagationFailureCount prometheus.Counter
	asyncReplicationPropagationObjectCount  prometheus.Counter
	asyncReplicationPropagationDuration     prometheus.Histogram
}

func NewMetrics(
	logger logrus.FieldLogger, prom *monitoring.PrometheusMetrics,
	className, shardName string,
) (*Metrics, error) {
	m := &Metrics{
		logger: logger,
	}

	if prom == nil {
		return m, nil
	}

	m.baseMetrics = prom

	if prom.Group {
		className = "n/a"
		shardName = "n/a"
		m.grouped = true
	}

	m.monitoring = true
	m.batchTime = prom.BatchTime.MustCurryWith(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
	})
	m.batchDeleteTime = prom.BatchDeleteTime.MustCurryWith(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
	})
	m.batchCount = prom.BatchCount.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
	})
	m.batchCountBytes = prom.BatchCountBytes.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
	})
	m.objectTime = prom.ObjectsTime.MustCurryWith(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
	})
	m.startupDurations = prom.StartupDurations.MustCurryWith(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
	})

	m.filteredVectorFilter = prom.QueriesFilteredVectorDurations.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
		"operation":  "filter",
	})

	m.filteredVectorVector = prom.QueriesFilteredVectorDurations.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
		"operation":  "vector",
	})

	m.filteredVectorObjects = prom.QueriesFilteredVectorDurations.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
		"operation":  "objects",
	})

	m.filteredVectorSort = prom.QueriesFilteredVectorDurations.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
		"operation":  "sort",
	})

	if prom.Registerer == nil {
		prom.Registerer = prometheus.DefaultRegisterer
	}

	// TODO: This is a temporary solution to avoid duplicating metrics registered
	// in the index package. it shall be removed once the index package metric is refactored
	// and to bring the metrics to the db package.
	shardsCount := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "weaviate_index_shards_total",
		Help: "Total number of shards per index status",
	}, []string{"status"}) // status: READONLY, INDEXING, LOADING, READY, SHUTDOWN

	shardStatusUpdateDurationsSeconds := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "weaviate_index_shard_status_update_duration_seconds",
		Help: "Time taken to update shard status in seconds",
	}, []string{"status"}) // status: READONLY, INDEXING, LOADING, READY, SHUTDOWN

	// Try to register metrics, reuse existing ones if already registered
	if err := prom.Registerer.Register(shardsCount); err != nil {
		var are prometheus.AlreadyRegisteredError
		if errors.As(err, &are) {
			shardsCount = are.ExistingCollector.(*prometheus.GaugeVec)
		}
	}

	if err := prom.Registerer.Register(shardStatusUpdateDurationsSeconds); err != nil {
		var are prometheus.AlreadyRegisteredError
		if errors.As(err, &are) {
			shardStatusUpdateDurationsSeconds = are.ExistingCollector.(*prometheus.HistogramVec)
		}
	}

	m.shardsCount = shardsCount
	m.shardStatusUpdateDurationsSeconds = shardStatusUpdateDurationsSeconds

	// Async Replication Metrics

	var err error

	m.asyncReplicationGoroutinesRunning, err = newGauge(prom.Registerer,
		"async_replication_goroutines_running",
		"Number of currently running async replication goroutines")
	if err != nil {
		return nil, err
	}

	m.asyncReplicationHashTreeInitRunning, err = newGauge(prom.Registerer,
		"async_replication_hashtree_init_running",
		"Number of currently running hashtree initializations")
	if err != nil {
		return nil, err
	}

	m.asyncReplicationHashTreeInitFailureCount, err = newCounter(prom.Registerer,
		"async_replication_hashtree_init_failure_count",
		"Count of async replication hashtree initialization failures")
	if err != nil {
		return nil, err
	}

	m.asyncReplicationHashTreeInitDuration, err = newHistogram(prom.Registerer,
		"async_replication_hashtree_init_duration_seconds",
		"Duration of hashtree initialization in seconds",
		defaultDurationBuckets)
	if err != nil {
		return nil, err
	}

	m.asyncReplicationIterationCount, err = newCounter(prom.Registerer,
		"async_replication_iteration_count",
		"Count of async replication comparison iterations")
	if err != nil {
		return nil, err
	}

	m.asyncReplicationIterationFailureCount, err = newCounter(prom.Registerer,
		"async_replication_iteration_failure_count",
		"Count of async replication iteration failures")
	if err != nil {
		return nil, err
	}

	m.asyncReplicationIterationDuration, err = newHistogram(prom.Registerer,
		"async_replication_iteration_duration_seconds",
		"Duration of async replication comparison iterations in seconds",
		defaultDurationBuckets)
	if err != nil {
		return nil, err
	}

	m.asyncReplicationPropagationCount, err = newCounter(prom.Registerer,
		"async_replication_propagation_count",
		"Count of async replication propagation executions")
	if err != nil {
		return nil, err
	}

	m.asyncReplicationPropagationFailureCount, err = newCounter(prom.Registerer,
		"async_replication_propagation_failure_count",
		"Count of async replication propagation failures")
	if err != nil {
		return nil, err
	}

	m.asyncReplicationPropagationObjectCount, err = newCounter(prom.Registerer,
		"async_replication_propagation_object_count",
		"Count of objects propagated by async replication")
	if err != nil {
		return nil, err
	}

	m.asyncReplicationPropagationDuration, err = newHistogram(prom.Registerer,
		"async_replication_propagation_duration_seconds",
		"Duration of async replication propagation in seconds",
		defaultDurationBuckets)
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

func newGauge(reg prometheus.Registerer, name, help string) (prometheus.Gauge, error) {
	g := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "weaviate",
		Name:      name,
		Help:      help,
	})
	if err := reg.Register(g); err != nil {
		var e prometheus.AlreadyRegisteredError
		if errors.As(err, &e) {
			if gauge, ok := e.ExistingCollector.(prometheus.Gauge); ok {
				return gauge, nil
			}
			return nil, fmt.Errorf("metric %s already registered but not as a Gauge", name)
		}
		return nil, err
	}
	return g, nil
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
			if histogram, ok := e.ExistingCollector.(prometheus.Histogram); ok {
				return histogram, nil
			}
			return nil, fmt.Errorf("metric %s already registered but not as a Histogram", name)
		}
		return nil, err
	}
	return h, nil
}

func (m *Metrics) UpdateShardStatus(old, new string) {
	if m.shardsCount == nil {
		return
	}

	if old != "" {
		m.shardsCount.WithLabelValues(old).Dec()
	}

	m.shardsCount.WithLabelValues(new).Inc()
}

func (m *Metrics) ObserveUpdateShardStatus(status string, duration time.Duration) {
	if m.shardStatusUpdateDurationsSeconds == nil {
		return
	}

	m.shardStatusUpdateDurationsSeconds.With(prometheus.Labels{"status": status}).Observe(float64(duration.Seconds()))
}

func (m *Metrics) DeleteShardLabels(class, shard string) {
	if m.grouped {
		// never delete the shared label, only individual ones
		return
	}

	m.baseMetrics.DeleteShard(class, shard)
}

func (m *Metrics) BatchObject(start time.Time, size int) {
	took := time.Since(start)
	m.logger.WithField("action", "batch_objects").
		WithField("batch_size", size).
		WithField("took", took).
		Tracef("object batch took %s", took)
}

func (m *Metrics) ObjectStore(start time.Time) {
	took := time.Since(start)
	m.logger.WithField("action", "store_object_store").
		WithField("took", took).
		Tracef("storing objects in KV/inverted store took %s", took)

	if !m.monitoring {
		return
	}

	m.batchTime.With(prometheus.Labels{"operation": "object_storage"}).
		Observe(float64(took / time.Millisecond))
}

func (m *Metrics) VectorIndex(start time.Time) {
	took := time.Since(start)
	m.logger.WithField("action", "store_vector_index").
		WithField("took", took).
		Tracef("storing objects vector index took %s", took)

	if !m.monitoring {
		return
	}

	m.batchTime.With(prometheus.Labels{"operation": "vector_storage"}).
		Observe(float64(took / time.Millisecond))
}

func (m *Metrics) PutObject(start time.Time) {
	took := time.Since(start)
	m.logger.WithField("action", "store_object_store_single_object_in_tx").
		WithField("took", took).
		Tracef("storing single object (complete) in KV/inverted took %s", took)

	if !m.monitoring {
		return
	}

	m.objectTime.With(prometheus.Labels{
		"operation": "put",
		"step":      "total",
	}).Observe(float64(took) / float64(time.Millisecond))
}

func (m *Metrics) PutObjectDetermineStatus(start time.Time) {
	took := time.Since(start)
	m.logger.WithField("action", "store_object_store_determine_status").
		WithField("took", took).
		Tracef("retrieving previous and determining status in KV took %s", took)

	if !m.monitoring {
		return
	}

	m.objectTime.With(prometheus.Labels{
		"operation": "put",
		"step":      "retrieve_previous_determine_status",
	}).Observe(float64(took) / float64(time.Millisecond))
}

func (m *Metrics) PutObjectUpsertObject(start time.Time) {
	took := time.Since(start)
	m.logger.WithField("action", "store_object_store_upsert_object_data").
		WithField("took", took).
		Tracef("storing object data in KV took %s", took)

	if !m.monitoring {
		return
	}

	m.objectTime.With(prometheus.Labels{
		"operation": "put",
		"step":      "upsert_object_store",
	}).Observe(float64(took) / float64(time.Millisecond))
}

func (m *Metrics) PutObjectUpdateInverted(start time.Time) {
	took := time.Since(start)
	m.logger.WithField("action", "store_object_store_update_inverted").
		WithField("took", took).
		Tracef("updating inverted index for single object took %s", took)

	if !m.monitoring {
		return
	}

	m.objectTime.With(prometheus.Labels{
		"operation": "put",
		"step":      "inverted_total",
	}).Observe(float64(took) / float64(time.Millisecond))
}

func (m *Metrics) InvertedDeleteOld(start time.Time) {
	took := time.Since(start)
	m.logger.WithField("action", "inverted_delete_old").
		WithField("took", took).
		Tracef("deleting old entries from inverted index %s", took)
	if !m.monitoring {
		return
	}

	m.objectTime.With(prometheus.Labels{
		"operation": "put",
		"step":      "inverted_delete",
	}).Observe(float64(took) / float64(time.Millisecond))
}

func (m *Metrics) InvertedDeleteDelta(start time.Time) {
	took := time.Since(start)
	m.logger.WithField("action", "inverted_delete_delta").
		WithField("took", took).
		Tracef("deleting delta entries from inverted index %s", took)
}

func (m *Metrics) InvertedExtend(start time.Time, propCount int) {
	took := time.Since(start)
	m.logger.WithField("action", "inverted_extend").
		WithField("took", took).
		WithField("prop_count", propCount).
		Tracef("extending inverted index took %s", took)

	if !m.monitoring {
		return
	}

	m.objectTime.With(prometheus.Labels{
		"operation": "put",
		"step":      "inverted_extend",
	}).Observe(float64(took) / float64(time.Millisecond))
}

func (m *Metrics) ShardStartup(start time.Time) {
	if !m.monitoring {
		return
	}

	took := time.Since(start)
	m.startupDurations.With(prometheus.Labels{
		"operation": "shard_total_init",
	}).Observe(float64(took) / float64(time.Millisecond))
}

func (m *Metrics) BatchDelete(start time.Time, op string) {
	if !m.monitoring {
		return
	}

	took := time.Since(start)
	m.batchDeleteTime.With(prometheus.Labels{
		"operation": op,
	}).Observe(float64(took) / float64(time.Millisecond))
}

func (m *Metrics) BatchCount(size int) {
	if !m.monitoring {
		return
	}

	m.batchCount.Add(float64(size))
}

func (m *Metrics) BatchCountBytes(size int64) {
	if !m.monitoring {
		return
	}

	m.batchCountBytes.Add(float64(size))
}

func (m *Metrics) FilteredVectorFilter(dur time.Duration) {
	if !m.monitoring {
		return
	}

	m.filteredVectorFilter.Observe(float64(dur) / float64(time.Millisecond))
}

func (m *Metrics) FilteredVectorVector(dur time.Duration) {
	if !m.monitoring {
		return
	}

	m.filteredVectorVector.Observe(float64(dur) / float64(time.Millisecond))
}

func (m *Metrics) FilteredVectorObjects(dur time.Duration) {
	if !m.monitoring {
		return
	}

	m.filteredVectorObjects.Observe(float64(dur) / float64(time.Millisecond))
}

func (m *Metrics) FilteredVectorSort(dur time.Duration) {
	if !m.monitoring {
		return
	}

	m.filteredVectorSort.Observe(float64(dur) / float64(time.Millisecond))
}

// --- Async Replication Lifecycle ---

func (m *Metrics) IncAsyncReplicationGoroutinesRunning() {
	if m.monitoring {
		m.asyncReplicationGoroutinesRunning.Inc()
	}
}

func (m *Metrics) DecAsyncReplicationGoroutinesRunning() {
	if m.monitoring {
		m.asyncReplicationGoroutinesRunning.Dec()
	}
}

// --- Hash Tree Init ---

func (m *Metrics) IncAsyncReplicationHashTreeInitRunning() {
	if m.monitoring {
		m.asyncReplicationHashTreeInitRunning.Inc()
	}
}

func (m *Metrics) DecAsyncReplicationHashTreeInitRunning() {
	if m.monitoring {
		m.asyncReplicationHashTreeInitRunning.Dec()
	}
}

func (m *Metrics) IncAsyncReplicationHashTreeInitFailure() {
	if m.monitoring {
		m.asyncReplicationHashTreeInitFailureCount.Inc()
	}
}

func (m *Metrics) ObserveAsyncReplicationHashTreeInitDuration(d time.Duration) {
	if m.monitoring {
		m.asyncReplicationHashTreeInitDuration.Observe(d.Seconds())
	}
}

// --- Iteration Metrics ---

func (m *Metrics) IncAsyncReplicationIterationCount() {
	if m.monitoring {
		m.asyncReplicationIterationCount.Inc()
	}
}

func (m *Metrics) IncAsyncReplicationIterationFailureCount() {
	if m.monitoring {
		m.asyncReplicationIterationFailureCount.Inc()
	}
}

func (m *Metrics) ObserveAsyncReplicationIterationDuration(d time.Duration) {
	if m.monitoring {
		m.asyncReplicationIterationDuration.Observe(d.Seconds())
	}
}

// --- Propagation Metrics ---

func (m *Metrics) IncAsyncReplicationPropagationCount() {
	if m.monitoring {
		m.asyncReplicationPropagationCount.Inc()
	}
}

func (m *Metrics) IncAsyncReplicationPropagationFailureCount() {
	if m.monitoring {
		m.asyncReplicationPropagationFailureCount.Inc()
	}
}

func (m *Metrics) AddAsyncReplicationPropagationObjectCount(n int) {
	if m.monitoring {
		m.asyncReplicationPropagationObjectCount.Add(float64(n))
	}
}

func (m *Metrics) ObserveAsyncReplicationPropagationDuration(d time.Duration) {
	if m.monitoring {
		m.asyncReplicationPropagationDuration.Observe(d.Seconds())
	}
}
